"""采集服务：把网络接收、组装状态机、持久化与发布三层接起来。

依赖全部通过构造函数注入（时钟、传输、重取源、Sink、Store），
服务本身只管编排，因此每一层都能独立测试，服务层用假传输做确定性测试，
另用真实回环 UDP 做端到端测试。
"""

import json
import signal
import time

from assembler import (
    ASSEMBLING, PUBLISHED,
    Assembler, DevicePolicy,
)
from protocol import ProtocolError, decode_frame
from transport import UdpTransport, parse_control, looks_like_status


class CollectorService:
    def __init__(self, config, store, sink, retransmit_sources=None,
                 transport=None, clock=None, logger=None):
        self.config = config
        self.store = store
        self.sink = sink
        self.clock = clock or time.monotonic
        self.logger = logger or _silent
        self.transport = transport
        self._stop = False

        policies = {
            name: DevicePolicy(
                address=tuple(policy["address"]) if policy.get("address") else None,
                expected_boot=policy.get("expected_boot"),
                retention_groups=policy.get("retention_groups", 16),
                max_reorder_span=policy.get("max_reorder_span", 32),
                max_groups=policy.get("max_groups", 64),
                group_ttl=policy.get("group_ttl", 30.0),
                max_count=policy.get("max_count", 256),
            )
            for name, policy in config.get("devices", {}).items()
        }
        self.assembler = Assembler(policies, clock=self.clock)
        self.retransmit_sources = retransmit_sources or {}

        self.refetch_interval = config.get("refetch_interval", 0.5)
        self.sweep_interval = config.get("sweep_interval", 1.0)
        self.reset_timers()
        self.protocol_errors = {}          # reason -> 次数
        self.pending_publish = []          # 崩溃恢复后待交付的完整组
        self._retrievers = {}              # device -> Retriever

    # ------------------------------------------------------------------
    def restore(self):
        """进程恢复：磁盘上的组按三种状态归位，完整但未交付的重新发布。"""
        counts = self.store.restore_into(self.assembler)
        for group in self.assembler.groups.values():
            if group.status == PUBLISHED:
                self.sink.deliver(group)  # 幂等，不会重复交付
            elif group.status == ASSEMBLING and group.is_complete:
                # 极端情况：状态已组齐但发布前崩溃。
                self.pending_publish.append(group)
        self.logger("restore", counts)
        return counts

    # ------------------------------------------------------------------
    def handle_datagram(self, raw, source_address):
        """网络 -> 协议 -> 组装 -> 持久化/发布，单数据报完整事务。"""
        try:
            preview = json.loads(raw.decode("utf-8"))
            device_name = preview.get("device") if isinstance(preview, dict) \
                else None
            # 设备策略的 max_count 在协议边界生效，超大组不入状态机。
            max_count = self.assembler.policy_for(device_name).max_count
            frame, payload = decode_frame(raw, max_count=max_count)
        except ProtocolError as exc:
            self.protocol_errors[exc.reason] = \
                self.protocol_errors.get(exc.reason, 0) + 1
            return None
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.protocol_errors["bad_json"] = \
                self.protocol_errors.get("bad_json", 0) + 1
            return None

        policy = self.assembler.policy_for(frame["device"])
        result = self.assembler.handle_frame(
            frame, payload, source_address=source_address, now=self.clock())

        # 连带放弃的组先落盘，保证晚到片无法翻案。
        for victim in result.abandoned_groups:
            self.store.save_group(victim)
        if result.session_changed or result.abandoned_groups:
            self._persist_sessions()

        if result.group is not None:
            self.store.save_group(result.group)

        if result.publish is not None:
            self._publish(result.publish)
        return result

    def _publish(self, group):
        if self.sink.deliver(group):
            self.logger("publish", group.key)
        # FileSink 已写记录；再保证组状态落盘。
        self.store.save_group(group)

    # ------------------------------------------------------------------
    def handle_control(self, raw, address):
        if looks_like_status(raw):
            self.transport.reply_status(address, self.snapshot())
            return
        try:
            command = parse_control(raw)
        except ProtocolError:
            return
        if command.get("command") == "reopen_boot":
            changed = self.assembler.reopen_boot(
                command["device"], command.get("boot"))
            if changed:
                self._persist_sessions()
            self.transport.reply_status(address, {
                "reopened": changed, **self.snapshot()})

    def _persist_sessions(self):
        self.store.save_sessions(self.assembler.sessions,
                                 self.assembler.active_session)

    # ------------------------------------------------------------------
    def reset_timers(self):
        """把周期任务计时对齐到当前注入时钟（测试与恢复后使用）。"""
        now = self.clock()
        self._last_refetch = now
        self._last_sweep = now

    def tick(self, now=None):
        """周期任务：超时淘汰 + 有上界缺片重取。"""
        now = self.clock() if now is None else now
        if now - self._last_sweep >= self.sweep_interval:
            self._last_sweep = now
            for group in self.assembler.sweep(now):
                self.store.save_group(group)
                self.logger("abandon", (group.key, group.abandon_reason))

        # 恢复后遗留的完整组在此交付。
        while self.pending_publish:
            self._publish(self.pending_publish.pop(0))

        if now - self._last_refetch >= self.refetch_interval:
            self._last_refetch = now
            self._run_refetch(now)

    def _run_refetch(self, now):
        for group, missing in self.assembler.due_refetches(now):
            source = self.retransmit_sources.get(group.device)
            if source is None:
                continue
            retriever = self._retrievers.setdefault(
                group.device,
                _build_retriever(source, self.assembler.policy_for(group.device),
                                 self.clock, self.config))
            for packet in retriever.fetch(group, missing):
                # 重取回来的报文走与网络报文完全相同的事务路径。
                self.handle_datagram(packet, source.bind_address)
            track = retriever.status(group.key)
            if track["exhausted"]:
                self.logger("retention_exhausted", (group.key, track))

    # ------------------------------------------------------------------
    def snapshot(self):
        snap = self.assembler.snapshot()
        snap["protocol_errors"] = dict(self.protocol_errors)
        snap["refetch"] = {
            device: {
                "/".join(str(part) for part in key): retriever.status(key)
                for key in retriever.tracked_keys()
            }
            for device, retriever in self._retrievers.items()
        }
        snap["published_records"] = len(self.sink.delivered_keys())
        return snap

    def run(self, transport=None, idle=0.05):
        transport = transport or self.transport or UdpTransport(
            tuple(self.config["data_addr"]),
            tuple(self.config["control_addr"]))
        self.transport = transport
        if not hasattr(transport, "data_socket") or transport.data_socket is None:
            transport.open()

        def stop(signum, frame):
            self._stop = True

        # 信号只能在主线程注册；服务以线程方式运行时（如演示/嵌入式）
        # 依靠 stop() 或 with 结束。
        try:
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            signals_installed = True
        except ValueError:
            signals_installed = False
        try:
            while not self._stop:
                datagrams, controls = transport.poll()
                for raw, address in datagrams:
                    self.handle_datagram(raw, address)
                for raw, address in controls:
                    self.handle_control(raw, address)
                self.tick()
                time.sleep(idle)
        finally:
            if signals_installed:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
            self._persist_sessions()

    def stop(self):
        self._stop = True


def _build_retriever(source, policy, clock, config):
    from retrieval import Retriever
    return Retriever(
        source,
        max_attempts=config.get("refetch_max_attempts", 3),
        timeout=policy.group_ttl,
        clock=clock,
    )


def _silent(*_args):
    pass


def load_config(path):
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))

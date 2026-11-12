"""真实回环 UDP 端到端测试：device.py + transport + service 全链路。

这些测试绑定 127.0.0.1 临时端口，无外部依赖；服务以注入时钟驱动，
轮询真实 socket，验证网络接收、组装状态、持久化发布三层的接缝。
"""

import json
import tempfile
import time
import unittest
from pathlib import Path

from assembler import ABANDONED, PUBLISHED
from collector import offline_status
from device import SyntheticDevice
from persistence import FileStore
from publisher import FileSink
from retrieval import MemoryRetransmit
from service import CollectorService
from source import Retention, packets
from transport import UdpTransport
from tests.test_assembler import make_fixture


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def wait_until(condition, timeout=3.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self.clock = FakeClock()
        self.transport = UdpTransport(("127.0.0.1", 0), ("127.0.0.1", 0))
        self.transport.open()
        self.device = SyntheticDevice(("127.0.0.1", 0))

    def tearDown(self):
        self.device.close()
        self.transport.close()
        self.tmp.cleanup()

    def _service(self, retention=None, **policy_kwargs):
        config = {
            "refetch_interval": 0.05,
            "refetch_max_attempts": 4,
            "sweep_interval": 0.05,
            "devices": {"bench-a": {"group_ttl": 5.0,
                                    "max_groups": 8, **policy_kwargs}},
        }
        store = FileStore(self.state_dir)
        sink = FileSink(store)
        sources = {}
        if retention is not None:
            sources["bench-a"] = MemoryRetransmit(
                retention, bind_address=self.device.address)
        service = CollectorService(config, store, sink,
                                   retransmit_sources=sources,
                                   transport=self.transport,
                                   clock=self.clock)
        return service, store, sink

    def _pump(self, service, rounds=5):
        for _ in range(rounds):
            datagrams, controls = self.transport.poll()
            for raw, address in datagrams:
                service.handle_datagram(raw, address)
            for raw, address in controls:
                service.handle_control(raw, address)
            service.tick()
            time.sleep(0.01)

    def test_shuffled_delivery_publishes_complete_record(self):
        service, store, sink = self._service()
        fixture = make_fixture(sequence=10)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture, shuffle=True)
        self._pump(service)
        self.assertEqual(len(sink.delivered_keys()), 1)
        record = json.loads(store.list_records()[0].read_text(encoding="utf-8"))
        expected = json.loads(packets(fixture)[0])["group_sha256"]
        self.assertEqual(record["group_sha256"], expected)
        self.assertEqual(record["length"], len("abc".encode()))

    def test_missing_part_is_refetched_and_completed(self):
        fixture = make_fixture(sequence=11)
        retention = Retention(fixture)
        service, _, sink = self._service(retention=retention)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture, drop=(1,))
        self._pump(service, rounds=2)
        key = ("bench-a", "boot-7", 11)
        self.assertEqual(sink.delivered_keys(), [])  # 缺片，尚未发布
        self.assertEqual(self.asm_status(service, key), "assembling")
        # 推进注入时钟跨过重取间隔，驱动一个周期：
        # 重取报文走与网络报文相同的事务路径。
        self.clock.advance(1.0)
        service.tick()
        self._pump(service, rounds=10)
        self.assertEqual(len(sink.delivered_keys()), 1)
        self.assertEqual(self.asm_status(service, key), PUBLISHED)
        # 重取确实经过了 Retriever 的有上界通道。
        track = service._retrievers["bench-a"].status(key)
        self.assertGreaterEqual(track["attempts"], 1)

    def test_missing_part_refetched_over_real_udp(self):
        """全链路真实 UDP：采集端发 missing 请求，设备线程应答。"""
        import threading
        from retrieval import UdpRetransmit

        config = {
            "refetch_interval": 0.1, "refetch_max_attempts": 4,
            "sweep_interval": 0.1,
            "devices": {"bench-a": {"group_ttl": 30.0, "max_groups": 8}},
        }
        store = FileStore(self.state_dir)
        service = CollectorService(
            config, store, FileSink(store),
            retransmit_sources={"bench-a": UdpRetransmit(
                self.device.address, wait=0.3)},
            transport=self.transport, clock=self.clock)
        fixture = make_fixture(sequence=12)
        self.device.stage(fixture, 16)
        # 只发两片，中间片缺失，设备后台应答重取请求。
        frames = packets(fixture)
        self.device.send_raw(frames[0], self.transport.bound_data_addr)
        self.device.send_raw(frames[2], self.transport.bound_data_addr)
        serve_thread = threading.Thread(
            target=self.device.serve, args=(3.0,), daemon=True)
        serve_thread.start()
        self._pump(service, rounds=2)
        self.clock.advance(1.0)
        service.tick()
        self._pump(service, rounds=12)
        key = ("bench-a", "boot-7", 12)
        self.assertEqual(self.asm_status(service, key), PUBLISHED)
        self.assertEqual(
            service.assembler.groups[key].payload_bytes(), b"abc")
        serve_thread.join(timeout=2.0)

    def test_sequence_wraparound_keeps_one_session(self):
        service, _, sink = self._service()
        for sequence in (65535, 0, 1):
            fixture = make_fixture(sequence=sequence, parts=("x",))
            self.device.send_measurement(
                self.transport.bound_data_addr, fixture)
        self._pump(service)
        self.assertEqual(len(sink.delivered_keys()), 3)
        sessions = service.assembler.sessions
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[("bench-a", "boot-7")].high_sequence, 1)

    def test_device_exit_leaves_incomplete_group_until_ttl(self):
        service, store, sink = self._service()
        fixture = make_fixture(sequence=12)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture, drop=(0, 1))
        self._pump(service)
        key = ("bench-a", "boot-7", 12)
        self.assertEqual(self.asm_status(service, key), "assembling")
        # 设备退出：没有重取源，推进时钟超过 TTL，组被超时淘汰。
        self.clock.advance(6.0)
        service.tick()
        self.assertEqual(self.asm_status(service, key), ABANDONED)
        # 磁盘状态同样标记为已放弃。
        offline = offline_status(self.state_dir)
        self.assertEqual(offline["totals"]["abandoned"], 1)

    def test_source_address_mismatch_is_rejected(self):
        service, _, sink = self._service(address=("127.0.0.1", 9999))
        fixture = make_fixture(sequence=13)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture)
        self._pump(service)
        self.assertEqual(sink.delivered_keys(), [])
        self.assertEqual(
            service.assembler.counters["rejected"], 3)

    def test_bad_datagrams_are_counted_and_never_enter_assembler(self):
        import base64
        import hashlib
        service, _, sink = self._service()
        # 非 JSON、坏 Base64、摘要不符三类坏帧都在协议层被挡。
        self.device.send_raw(b"not-json", self.transport.bound_data_addr)
        good = json.loads(packets(make_fixture(sequence=15))[0])
        bad_b64 = dict(good, payload="@@@")
        self.device.send_raw(json.dumps(bad_b64).encode(),
                             self.transport.bound_data_addr)
        bad_digest = dict(good, part_sha256="f" * 64)
        self.device.send_raw(json.dumps(bad_digest).encode(),
                             self.transport.bound_data_addr)
        self._pump(service)
        self.assertEqual(service.protocol_errors.get("bad_json"), 1)
        self.assertEqual(service.protocol_errors.get("bad_base64"), 1)
        self.assertEqual(service.protocol_errors.get("digest_mismatch"), 1)
        self.assertEqual(sink.delivered_keys(), [])
        self.assertEqual(service.assembler.groups, {})

    def test_oversized_group_is_rejected_at_boundary(self):
        service, _, sink = self._service(max_count=4)
        frame = json.loads(packets(make_fixture(sequence=16))[0])
        frame["count"] = 5  # 超过设备策略 max_count
        self.device.send_raw(json.dumps(frame).encode(),
                             self.transport.bound_data_addr)
        self._pump(service)
        self.assertEqual(service.protocol_errors.get("oversized_group"), 1)
        self.assertEqual(sink.delivered_keys(), [])

    def test_status_query_over_real_udp(self):
        import socket
        service, _, _ = self._service()
        fixture = make_fixture(sequence=14)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture)
        self._pump(service)
        # 查询客户端与服务在同线程：先发出查询，让服务轮询处理后再收应答。
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
            channel.settimeout(2.0)
            channel.bind(("127.0.0.1", 0))
            channel.sendto(b'{"query":"status"}',
                           self.transport.bound_control_addr)
            self._pump(service, rounds=1)
            data, _ = channel.recvfrom(65535)
        snapshot = json.loads(data.decode("utf-8"))
        self.assertEqual(snapshot["counters"]["published"], 1)
        self.assertEqual(len(snapshot["groups"]), 1)

    def test_process_restart_rebuilds_three_statuses(self):
        fixture_done = make_fixture(sequence=20, parts=("z",))
        fixture_open = make_fixture(sequence=21, parts=("a", "b"))
        # 三片组且扣下末片：冲突帧到达时组仍未组齐，才能验证隔离。
        fixture_bad = make_fixture(sequence=22, parts=("q", "r", "s"))

        first, store, _ = self._service()
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture_done)
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture_open, drop=(1,))
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture_bad, drop=(2,))
        self._pump(first)
        # 制造一个冲突隔离组（同片号、自洽摘要、不同内容）。
        import base64
        import hashlib
        evil = b"EVIL"
        frame = json.loads(packets(fixture_bad)[0])
        frame["payload"] = base64.b64encode(evil).decode()
        frame["part_sha256"] = hashlib.sha256(evil).hexdigest()
        self.device.send_raw(json.dumps(frame).encode(),
                             self.transport.bound_data_addr)
        self._pump(first)

        # 新进程：同目录恢复，组装器从磁盘重建。
        second, _, sink2 = self._service()
        counts = second.restore()
        self.assertEqual(counts[PUBLISHED], 1)
        self.assertEqual(counts["assembling"], 1)
        self.assertEqual(counts[ABANDONED], 1)
        # 已发布记录不重复交付；晚到片不能翻案。
        self.assertEqual(len(sink2.delivered_keys()), 1)
        late = second.handle_datagram(
            packets(fixture_bad)[1], self.device.address)
        self.assertEqual(late.status, ABANDONED)
        # 未组齐的组恢复后仍可补齐。
        self.device.send_measurement(
            self.transport.bound_data_addr, fixture_open)
        self._pump(second)
        self.assertEqual(len(sink2.delivered_keys()), 2)

    @staticmethod
    def asm_status(service, key):
        return service.assembler.groups[key].status


if __name__ == "__main__":
    unittest.main()

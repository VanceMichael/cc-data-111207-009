"""组装状态机：按 (device, boot, sequence) 归组并维护有界缓存。

本模块不碰 socket 与磁盘，所有时间由外部时钟注入，所有策略来自
DevicePolicy，因此乱序、回绕、冲突、上界淘汰等都可在单元测试中确定复现。

关键约定（见 docs/数据约定.md）：
- 序号是 16 位、按启动代次解释的连续观察；跨越半个序号空间即失去
  连续性证据，整个代次失败，必须重新确认启动代次。
- 乱序跨度至多 max_reorder_span，源端只保留最近 retention_groups 组。
- 同片重传（内容相同）幂等；同片内容冲突、总片数改变、整组摘要不符
  的组被隔离，永远不会发布。
- 只有全部片齐全且摘要一致才发布；重复收齐也只发布一次。
"""

from dataclasses import dataclass, field

from protocol import SEQUENCE_MOD, group_key

# 组的生命周期状态。
ASSEMBLING = "assembling"
PUBLISHED = "published"
ABANDONED = "abandoned"

# 启动代次会话状态。
SESSION_ACTIVE = "active"
SESSION_FAILED = "failed"          # 连续性证据丢失，需要新启动代次
SESSION_SUPERSEDED = "superseded"  # 设备已用新 boot 重启

# 放弃原因。
REASON_TTL = "ttl_expired"
REASON_EVICTED = "capacity_evicted"
REASON_CONFLICT = "fragment_conflict"
REASON_DIGEST = "group_digest_mismatch"
REASON_COUNT_CHANGED = "count_changed"
REASON_SESSION_FAILED = "session_failed"
REASON_BOOT_SUPERSEDED = "boot_superseded"


def sequence_delta(previous, current):
    """有符号序号差，结果落在 (-32768, 32767]。

    正数表示 current 在 previous 之后（含回绕），负数表示更早。
    差值等于半个空间（+32768）视为歧义，按不可判断处理。
    """
    delta = (current - previous) % SEQUENCE_MOD
    if delta == SEQUENCE_MOD // 2:
        return SEQUENCE_MOD // 2  # 歧义点，调用方必须拒绝
    if delta > SEQUENCE_MOD // 2:
        return delta - SEQUENCE_MOD
    return delta


@dataclass
class DevicePolicy:
    """单个设备的协议上界与来源约束。"""

    address: tuple | None = None          # 允许的来源 (ip, port)；None 表示不限制
    expected_boot: str | None = None      # 固定启动代次；None 表示允许代次演进
    retention_groups: int = 16            # 源端暂存的最近组数
    max_reorder_span: int = 32            # 最大乱序跨度（组）
    max_groups: int = 64                  # 本设备在采集端缓存的组上界
    retain_finished: int = 128            # 内存中保留的已完成组上界
    group_ttl: float = 30.0               # 组从最后活动起的存活秒数
    max_count: int = 256                  # 本设备单组总片数上界


@dataclass
class Part:
    index: int
    payload: bytes
    part_sha256: str


@dataclass
class Group:
    device: str
    boot: str
    sequence: int
    count: int
    group_sha256: str
    parts: dict = field(default_factory=dict)
    status: str = ASSEMBLING
    abandon_reason: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    deadline: float = 0.0

    @property
    def key(self):
        return group_key(self.device, self.boot, self.sequence)

    @property
    def missing_indexes(self):
        return [i for i in range(self.count) if i not in self.parts]

    @property
    def is_complete(self):
        return len(self.parts) == self.count

    def payload_bytes(self):
        return b"".join(self.parts[i].payload for i in range(self.count))


@dataclass
class Session:
    device: str
    boot: str
    state: str = SESSION_ACTIVE
    high_sequence: int | None = None  # 已观察到的最大序号（回绕空间内）

    @property
    def key(self):
        return self.device, self.boot


@dataclass
class FrameResult:
    """handle_frame 的判定结果，供服务层做持久化与发布。

    abandoned_groups 收集本次处理中被连带放弃的组（容量淘汰、会话失败、
    代次更替），服务层必须把它们与 group 一并落盘。
    """

    accepted: bool
    status: str                       # accepted / rejected / abandoned / published
    reason: str | None = None
    group: Group | None = None
    publish: Group | None = None      # 非 None 时该组已具备发布条件（仅触发一次）
    session_changed: bool = False
    abandoned_groups: list = field(default_factory=list)


class Assembler:
    def __init__(self, policies=None, clock=None):
        self.policies = dict(policies or {})
        self.clock = clock or _system_clock
        self.groups = {}               # group_key -> Group
        self.sessions = {}             # (device, boot) -> Session
        self.active_session = {}       # device -> Session
        # 已终结（发布/放弃）但组对象已被内存回收的键；晚到片到此即止步，
        # 不能把历史结论重新组装成新组。按设备有界保留。
        self.finished_keys = {}        # device -> OrderedDict-like dict
        self.counters = self._empty_counters()

    @staticmethod
    def _empty_counters():
        return {
            "received": 0,
            "accepted": 0,
            "rejected": 0,
            "duplicate_parts": 0,
            "published": 0,
            "abandoned": 0,
            "quarantined": 0,
        }

    def policy_for(self, device):
        return self.policies.get(device, DevicePolicy())

    # ------------------------------------------------------------------
    # 入口：处理一个已通过协议层结构校验的帧
    # ------------------------------------------------------------------
    def handle_frame(self, frame, payload, source_address=None, now=None):
        now = self.clock() if now is None else now
        self.counters["received"] += 1
        device = frame["device"]
        boot = frame["boot"]
        sequence = frame["sequence"]
        policy = self.policy_for(device)

        address_reason, _ = self._check_address(policy, source_address)
        if address_reason:
            return self._reject(address_reason)

        boot_reason, boot_effects = self._check_boot(device, boot, policy, now)
        if boot_reason:
            return self._reject(boot_reason)

        session = self.sessions[(device, boot)]
        span_reason = self._check_continuity(session, sequence, policy)
        if span_reason:
            abandoned = self._fail_session(session, now)
            result = FrameResult(False, "rejected", reason=span_reason,
                                 session_changed=True,
                                 abandoned_groups=boot_effects + abandoned)
            return result

        key = group_key(device, boot, sequence)
        group = self.groups.get(key)
        if group is not None and group.status != ASSEMBLING:
            # 已发布或已放弃的组：晚到片不能改变结论。
            return FrameResult(True, group.status, group=group,
                               abandoned_groups=boot_effects)
        if group is None and key in self.finished_keys.get(device, {}):
            # 组对象已被内存回收，但历史结论（发布/放弃）仍然有效，
            # 晚到片不能让它作为新组复活。
            return FrameResult(True, "terminal", reason="already_terminal")

        side_effects = list(boot_effects)
        if group is None:
            evicted = self._ensure_capacity(device, boot, policy, now)
            if evicted is not None:
                side_effects.append(evicted)
            group = Group(
                device=device, boot=boot, sequence=sequence,
                count=frame["count"], group_sha256=frame["group_sha256"],
                created_at=now, updated_at=now, deadline=now + policy.group_ttl,
            )
            self.groups[key] = group

        conflict = self._absorb_part(group, frame, payload, now, policy)
        if conflict:
            self._abandon(group, conflict, now)
            return FrameResult(False, "abandoned", reason=conflict, group=group,
                               abandoned_groups=side_effects + [group])

        self.counters["accepted"] += 1
        group.updated_at = now
        group.deadline = now + policy.group_ttl

        if group.is_complete:
            digest_reason = self._verify_group(group)
            if digest_reason:
                self._abandon(group, digest_reason, now)
                return FrameResult(False, "abandoned", reason=digest_reason,
                                   group=group,
                                   abandoned_groups=side_effects + [group])
            self.counters["published"] += 1
            group.status = PUBLISHED
            return FrameResult(True, "published", group=group, publish=group,
                               abandoned_groups=side_effects)

        return FrameResult(True, "accepted", group=group,
                           abandoned_groups=side_effects)

    # ------------------------------------------------------------------
    def _check_address(self, policy, source_address):
        if policy.address is None or source_address is None:
            return None, []
        if source_address != policy.address:
            return "source_address_mismatch", []
        return None, []

    def _check_boot(self, device, boot, policy, now):
        """返回 (拒绝原因, 连带放弃的组)。原因非 None 时必须拒绝。"""
        if policy.expected_boot is not None and boot != policy.expected_boot:
            return "unknown_boot", []

        session_key = (device, boot)
        if session_key in self.sessions:
            session = self.sessions[session_key]
            if session.state == SESSION_FAILED:
                return "session_failed_needs_new_boot", []
            return None, []

        # 首次见到该 (device, boot)。
        session = Session(device, boot)
        self.sessions[session_key] = session
        effects = []
        previous = self.active_session.get(device)
        if previous is not None and previous.state == SESSION_ACTIVE:
            # 设备重启：旧代次的未完成组不可能再补齐，放弃之。
            previous.state = SESSION_SUPERSEDED
            for group in self._groups_of(device, previous.boot):
                if group.status == ASSEMBLING:
                    self._abandon(group, REASON_BOOT_SUPERSEDED, now)
                    effects.append(group)
        self.active_session[device] = session
        return None, effects

    def _check_continuity(self, session, sequence, policy):
        """按启动代次内的连续观察判断序号位置。

        - 半个序号空间（+32768）是显式歧义点，无法区分前进与回退；
        - 无论向前还是向后，差距超过 max_reorder_span 都意味着中间隔着
          无法由乱序解释的空档，连续位置证据不足；
        上述两类都让整个代次失败，请求新的启动代次，而不是凭接收时刻猜测。
        """
        if session.high_sequence is None:
            session.high_sequence = sequence
            return None
        delta = sequence_delta(session.high_sequence, sequence)
        if delta == SEQUENCE_MOD // 2:
            return "sequence_ambiguous"
        if delta > 0:
            if delta > policy.max_reorder_span:
                return "forward_gap_exceeded"
            session.high_sequence = sequence
            return None
        # 更早的序号：乱序跨度有界，超过即无法判断连续位置。
        if -delta > policy.max_reorder_span:
            return "reorder_span_exceeded"
        return None

    def _absorb_part(self, group, frame, payload, now, policy):
        if frame["count"] != group.count:
            return REASON_COUNT_CHANGED
        if frame["group_sha256"] != group.group_sha256:
            return REASON_DIGEST
        index = frame["index"]
        if index in group.parts:
            existing = group.parts[index]
            if (existing.payload == payload
                    and existing.part_sha256 == frame["part_sha256"]):
                # 同片重传：正常且幂等。
                self.counters["duplicate_parts"] += 1
                return None
            return REASON_CONFLICT
        group.parts[index] = Part(index, payload, frame["part_sha256"])
        return None

    def _verify_group(self, group):
        import hashlib
        if hashlib.sha256(group.payload_bytes()).hexdigest() != group.group_sha256:
            return REASON_DIGEST
        return None

    def _ensure_capacity(self, device, boot, policy, now):
        active = [g for g in self._groups_of(device, boot)
                  if g.status == ASSEMBLING]
        if len(active) < policy.max_groups:
            return None
        # 上界：淘汰最久未活动的未完成组，离线设备不能无限占缓存。
        victim = min(active, key=lambda g: g.updated_at)
        self._abandon(victim, REASON_EVICTED, now)
        return victim

    def _abandon(self, group, reason, now):
        group.status = ABANDONED
        group.abandon_reason = reason
        group.deadline = now
        self.counters["abandoned"] += 1
        if reason in (REASON_CONFLICT, REASON_DIGEST, REASON_COUNT_CHANGED):
            self.counters["quarantined"] += 1

    def _fail_session(self, session, now):
        """连续性证据丢失：会话失败并隔离其全部未完成组，返回这些组。"""
        session.state = SESSION_FAILED
        session.high_sequence = None
        abandoned = []
        for group in self._groups_of(session.device, session.boot):
            if group.status == ASSEMBLING:
                self._abandon(group, REASON_SESSION_FAILED, now)
                abandoned.append(group)
        return abandoned

    def _groups_of(self, device, boot):
        if boot is None:
            return [g for g in self.groups.values() if g.device == device]
        return [g for g in self.groups.values()
                if g.device == device and g.boot == boot]

    def _reject(self, reason):
        self.counters["rejected"] += 1
        return FrameResult(False, "rejected", reason=reason)

    # ------------------------------------------------------------------
    # 重取与淘汰（由服务层周期性调用）
    # ------------------------------------------------------------------
    def due_refetches(self, now=None):
        """返回 (group, indexes) 列表：TTL 内仍缺片、且落在源端暂存
        最近 retention_groups 组之内的组。超出暂存窗口的组不再请求。"""
        now = self.clock() if now is None else now
        due = []
        for session in self.sessions.values():
            if session.state != SESSION_ACTIVE or session.high_sequence is None:
                continue
            window = set()
            high = session.high_sequence
            for back in range(self.policy_for(session.device).retention_groups):
                window.add((high - back) % SEQUENCE_MOD)
            for group in self._groups_of(session.device, session.boot):
                if group.status != ASSEMBLING:
                    continue
                if group.sequence not in window:
                    continue  # 源端已不可能暂存，等 TTL 淘汰
                missing = group.missing_indexes
                if missing:
                    due.append((group, missing))
        return due

    def sweep(self, now=None):
        """周期回收。

        - 超过 TTL 的未完成组放弃；
        - 已完成（发布/放弃）组若超过每设备 retain_finished 上界，
          按最后活动时间淘汰最旧的，仅释放内存；磁盘记录由持久层保留。
        返回被 TTL 放弃的组列表。
        """
        now = self.clock() if now is None else now
        expired = []
        for group in list(self.groups.values()):
            if group.status == ASSEMBLING and now >= group.deadline:
                self._abandon(group, REASON_TTL, now)
                expired.append(group)
        self._prune_finished()
        return expired

    def _prune_finished(self):
        for device, policy in self.policies.items():
            finished = [g for g in self._groups_of(device, None)
                        if g.status != ASSEMBLING]
            excess = len(finished) - policy.retain_finished
            if excess <= 0:
                continue
            victims = sorted(finished, key=lambda g: g.updated_at)[:excess]
            remembered = self.finished_keys.setdefault(device, {})
            for victim in victims:
                self.groups.pop(victim.key, None)
                # 记录终结键并维持有界：字典按插入序，超界丢弃最旧的。
                remembered[victim.key] = victim.status
            while len(remembered) > policy.retain_finished:
                oldest = next(iter(remembered))
                remembered.pop(oldest)

    def reopen_boot(self, device, boot=None):
        """显式确认新启动代次：把失败会话清掉，允许同 boot 重新开始。

        用于 expected_boot 固定、设备重启后需要人工/上游确认的场景。
        """
        target = (device, boot) if boot else None
        for key, session in list(self.sessions.items()):
            if key[0] != device:
                continue
            if target is not None and key != target:
                continue
            if session.state == SESSION_FAILED:
                del self.sessions[key]
                if self.active_session.get(device) is session:
                    del self.active_session[device]
                return True
        return False

    # ------------------------------------------------------------------
    # 进程恢复：由持久层把磁盘上的组灌回
    # ------------------------------------------------------------------
    def restore_group(self, group):
        self.groups[group.key] = group
        session_key = (group.device, group.boot)
        session = self.sessions.get(session_key)
        if session is None:
            session = Session(group.device, group.boot)
            self.sessions[session_key] = session
            self.active_session.setdefault(group.device, session)
        if session.high_sequence is None:
            session.high_sequence = group.sequence
        else:
            delta = sequence_delta(session.high_sequence, group.sequence)
            if delta > 0 and delta != SEQUENCE_MOD // 2:
                session.high_sequence = group.sequence
        if group.status == PUBLISHED:
            self.counters["published"] += 1
            self.finished_keys.setdefault(group.device, {})[group.key] = \
                PUBLISHED
        elif group.status == ABANDONED:
            self.counters["abandoned"] += 1
            self.finished_keys.setdefault(group.device, {})[group.key] = \
                ABANDONED
            if group.abandon_reason in (REASON_CONFLICT, REASON_DIGEST,
                                        REASON_COUNT_CHANGED):
                self.counters["quarantined"] += 1

    # ------------------------------------------------------------------
    def snapshot(self):
        """状态查询用的纯数据视图。"""
        now = self.clock()
        groups = []
        for group in sorted(self.groups.values(),
                            key=lambda g: (g.device, g.boot, g.sequence)):
            groups.append({
                "device": group.device,
                "boot": group.boot,
                "sequence": group.sequence,
                "status": group.status,
                "count": group.count,
                "held_parts": len(group.parts),
                "missing": group.missing_indexes,
                "abandon_reason": group.abandon_reason,
                "ttl_remaining": round(max(0.0, group.deadline - now), 3),
            })
        sessions = [
            {"device": s.device, "boot": s.boot, "state": s.state,
             "high_sequence": s.high_sequence}
            for s in sorted(self.sessions.values(), key=lambda s: s.key)
        ]
        return {
            "now": now,
            "counters": dict(self.counters),
            "sessions": sessions,
            "groups": groups,
        }


def _system_clock():
    import time
    return time.monotonic()

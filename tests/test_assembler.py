"""组装状态机测试：全部用注入时钟与手工构造的帧，确定性、无 I/O。"""

import base64
import hashlib
import unittest

from assembler import (
    ABANDONED, ASSEMBLING, PUBLISHED,
    SESSION_FAILED, SESSION_SUPERSEDED,
    Assembler, DevicePolicy,
    REASON_CONFLICT, REASON_COUNT_CHANGED, REASON_DIGEST,
    REASON_EVICTED, REASON_TTL,
)
from protocol import decode_frame
from source import packets


def make_fixture(sequence=10, boot="boot-7", device="bench-a",
                 parts=("a", "b", "c")):
    return {"device": device, "boot": boot, "sequence": sequence,
            "parts": list(parts)}


def frame_of(fixture, index):
    raw = packets(fixture)[index]
    frame, payload = decode_frame(raw)
    return frame, payload


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def build_assembler(clock, **policy_kwargs):
    policy = DevicePolicy(**policy_kwargs)
    return Assembler({"bench-a": policy}, clock=clock), policy


class SequenceTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.asm, _ = build_assembler(self.clock)

    def test_ordered_group_publishes_once(self):
        fixture = make_fixture(sequence=10)
        self.asm.handle_frame(*frame_of(fixture, 0))
        self.asm.handle_frame(*frame_of(fixture, 1))
        # 组齐前的同片重传计入重复片且幂等。
        repeat = self.asm.handle_frame(*frame_of(fixture, 1))
        self.assertEqual(repeat.status, "accepted")
        self.assertEqual(self.asm.counters["duplicate_parts"], 1)
        # 最后一片到达，整组发布。
        last = self.asm.handle_frame(*frame_of(fixture, 2))
        self.assertEqual(last.status, PUBLISHED)
        self.assertIsNotNone(last.publish)
        # 已发布组的晚到片不能再次发布。
        final = self.asm.handle_frame(*frame_of(fixture, 0))
        self.assertEqual(final.status, PUBLISHED)
        self.assertIsNone(final.publish)
        self.assertEqual(self.asm.counters["published"], 1)

    def test_reorder_within_span_is_accepted(self):
        fixture = make_fixture(sequence=10)
        for index in (2, 0, 1):
            result = self.asm.handle_frame(*frame_of(fixture, index))
            self.assertTrue(result.accepted)
        group = self.asm.groups[("bench-a", "boot-7", 10)]
        self.assertEqual(group.status, PUBLISHED)
        self.assertEqual(group.payload_bytes(), b"abc")

    def test_wraparound_sequence_is_continuous(self):
        # 65535 -> 0 -> 1 应被解释为同一代次内的连续推进。
        for sequence in (65535, 0, 1):
            fixture = make_fixture(sequence=sequence, parts=("x",))
            result = self.asm.handle_frame(*frame_of(fixture, 0))
            self.assertTrue(result.accepted, result.reason)
        session = self.asm.sessions[("bench-a", "boot-7")]
        self.assertEqual(session.high_sequence, 1)
        self.assertEqual(self.asm.counters["published"], 3)

    def test_half_space_jump_is_ambiguous_and_fails_boot(self):
        first = make_fixture(sequence=0, parts=("x",))
        self.asm.handle_frame(*frame_of(first, 0))
        far = make_fixture(sequence=32768, parts=("y",))
        result = self.asm.handle_frame(*frame_of(far, 0))
        self.assertFalse(result.accepted)
        self.assertEqual(result.reason, "sequence_ambiguous")
        self.assertEqual(
            self.asm.sessions[("bench-a", "boot-7")].state, SESSION_FAILED)
        # 失败代次的后续帧一律拒绝，必须换新 boot。
        again = self.asm.handle_frame(*frame_of(first, 0))
        self.assertEqual(again.reason, "session_failed_needs_new_boot")

    def test_large_forward_gap_fails_boot(self):
        # 从 201 跳到 32768：未到半空间歧义点，但远超 32 组乱序跨度，
        # 中间的连续位置无法由乱序解释，必须停止并请求新 boot。
        current = make_fixture(sequence=201, parts=("x",))
        self.asm.handle_frame(*frame_of(current, 0))
        far = make_fixture(sequence=32768, parts=("y",))
        result = self.asm.handle_frame(*frame_of(far, 0))
        self.assertEqual(result.reason, "forward_gap_exceeded")
        self.assertEqual(
            self.asm.sessions[("bench-a", "boot-7")].state, SESSION_FAILED)
        self.assertNotIn(("bench-a", "boot-7", 32768), self.asm.groups)

    def test_reorder_beyond_span_fails_boot(self):
        current = make_fixture(sequence=33, parts=("x",))
        self.asm.handle_frame(*frame_of(current, 0))
        late = make_fixture(sequence=0, parts=("y",))  # 回看 33 组，超出跨度 32
        result = self.asm.handle_frame(*frame_of(late, 0))
        self.assertEqual(result.reason, "reorder_span_exceeded")
        self.assertEqual(
            self.asm.sessions[("bench-a", "boot-7")].state, SESSION_FAILED)

    def test_reorder_at_exact_span_boundary_accepted(self):
        current = make_fixture(sequence=31, parts=("x",))
        self.asm.handle_frame(*frame_of(current, 0))
        late = make_fixture(sequence=0, parts=("y",))  # 回看 31 组
        result = self.asm.handle_frame(*frame_of(late, 0))
        self.assertTrue(result.accepted, result.reason)


class ConflictTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.asm, _ = build_assembler(self.clock)

    def test_same_index_conflicting_payload_is_quarantined(self):
        fixture = make_fixture()
        self.asm.handle_frame(*frame_of(fixture, 0))
        frame, payload = frame_of(fixture, 0)
        frame["payload"] = base64.b64encode(b"different").decode()
        frame["part_sha256"] = hashlib.sha256(b"different").hexdigest()
        result = self.asm.handle_frame(frame, b"different")
        self.assertFalse(result.accepted)
        self.assertEqual(result.reason, REASON_CONFLICT)
        group = self.asm.groups[("bench-a", "boot-7", 10)]
        self.assertEqual(group.status, ABANDONED)
        self.assertEqual(self.asm.counters["quarantined"], 1)
        # 晚到的合法片不能让隔离组复活。
        again = self.asm.handle_frame(*frame_of(fixture, 1))
        self.assertEqual(again.status, ABANDONED)
        self.assertIsNone(again.publish)

    def test_count_change_is_quarantined(self):
        fixture = make_fixture(parts=("a", "b", "c"))
        self.asm.handle_frame(*frame_of(fixture, 0))
        other = make_fixture(parts=("a", "b"))
        frame, payload = frame_of(other, 1)
        result = self.asm.handle_frame(frame, payload)
        self.assertEqual(result.reason, REASON_COUNT_CHANGED)
        self.assertEqual(
            self.asm.groups[("bench-a", "boot-7", 10)].status, ABANDONED)

    def test_group_digest_mismatch_is_quarantined(self):
        fixture = make_fixture()
        frames = [frame_of(fixture, i) for i in range(3)]
        # 伪造 group_sha256 但保持各片自洽：构造两片一组的假测量。
        fake = make_fixture(parts=("a", "b"))
        f0, p0 = frame_of(fake, 0)
        f1, p1 = frame_of(fake, 1)
        f0["group_sha256"] = "f" * 64
        f1["group_sha256"] = "f" * 64
        self.asm.handle_frame(f0, p0)
        result = self.asm.handle_frame(f1, p1)
        self.assertEqual(result.reason, REASON_DIGEST)
        self.assertEqual(
            self.asm.groups[("bench-a", "boot-7", 10)].status, ABANDONED)


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.asm, self.policy = build_assembler(
            self.clock, max_groups=3, group_ttl=10.0)

    def test_capacity_evicts_oldest_incomplete_group(self):
        # 每组只发一片，保持未完成；第 4 组到来时淘汰最久未活动的组。
        for sequence in range(4):
            fixture = make_fixture(sequence=sequence)
            result = self.asm.handle_frame(*frame_of(fixture, 0))
            self.assertTrue(result.accepted)
        abandoned = [g for g in self.asm.groups.values()
                     if g.status == ABANDONED]
        self.assertEqual(len(abandoned), 1)
        self.assertEqual(abandoned[0].sequence, 0)
        self.assertEqual(abandoned[0].abandon_reason, REASON_EVICTED)
        assembling = [g for g in self.asm.groups.values()
                      if g.status == ASSEMBLING]
        self.assertEqual(len(assembling), 3)

    def test_ttl_sweep_abandons_stale_group(self):
        fixture = make_fixture(sequence=5)
        self.asm.handle_frame(*frame_of(fixture, 0))
        self.clock.advance(11.0)
        expired = self.asm.sweep()
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].abandon_reason, REASON_TTL)
        # 晚到片不能把超时放弃的组重新变成成功。
        late = self.asm.handle_frame(*frame_of(fixture, 1))
        self.assertEqual(late.status, ABANDONED)
        self.assertIsNone(late.publish)

    def test_finished_groups_are_memory_bounded(self):
        asm, _ = build_assembler(self.clock, retain_finished=2)
        for sequence in range(4):
            fixture = make_fixture(sequence=sequence, parts=("z",))
            asm.handle_frame(*frame_of(fixture, 0))
            self.clock.advance(1.0)
        asm.sweep()  # 触发完成组回收
        remaining = sorted(sequence for _, _, sequence in asm.groups)
        # 只保留最新的两个已发布组，内存无界增长被阻止。
        self.assertEqual(remaining, [2, 3])
        # 被回收的 seq=0 仍是终结键：晚到片不能让它作为新组复活。
        late = asm.handle_frame(*frame_of(make_fixture(sequence=0), 0))
        self.assertEqual(late.status, "terminal")
        self.assertNotIn(("bench-a", "boot-7", 0), asm.groups)

    def test_published_group_is_not_evicted(self):
        for sequence in range(3):
            fixture = make_fixture(sequence=sequence, parts=("z",))
            self.asm.handle_frame(*frame_of(fixture, 0))
        # 再发未完成组时，已发布组不参与容量淘汰。
        extra = make_fixture(sequence=5, parts=("a", "b"))
        result = self.asm.handle_frame(*frame_of(extra, 0))
        self.assertTrue(result.accepted)
        for sequence in range(3):
            self.assertEqual(
                self.asm.groups[("bench-a", "boot-7", sequence)].status,
                PUBLISHED)


class PolicyTests(unittest.TestCase):
    def test_source_address_mismatch_rejected(self):
        clock = FakeClock()
        asm, _ = build_assembler(clock, address=("127.0.0.1", 9000))
        fixture = make_fixture()
        result = asm.handle_frame(*frame_of(fixture, 0),
                                  source_address=("127.0.0.1", 1))
        self.assertEqual(result.reason, "source_address_mismatch")
        ok = asm.handle_frame(*frame_of(fixture, 0),
                              source_address=("127.0.0.1", 9000))
        self.assertTrue(ok.accepted)

    def test_unknown_boot_rejected_when_pinned(self):
        clock = FakeClock()
        asm, _ = build_assembler(clock, expected_boot="boot-7")
        fixture = make_fixture(boot="boot-8")
        result = asm.handle_frame(*frame_of(fixture, 0))
        self.assertEqual(result.reason, "unknown_boot")

    def test_new_boot_supersedes_old_incomplete_groups(self):
        clock = FakeClock()
        asm, _ = build_assembler(clock)
        old = make_fixture(boot="boot-7", sequence=3)
        asm.handle_frame(*frame_of(old, 0))  # 未完成
        new = make_fixture(boot="boot-8", sequence=0)
        result = asm.handle_frame(*frame_of(new, 0))
        self.assertTrue(result.accepted)
        self.assertEqual(len(result.abandoned_groups), 1)
        victim = result.abandoned_groups[0]
        self.assertEqual(victim.boot, "boot-7")
        self.assertEqual(victim.abandon_reason, "boot_superseded")
        self.assertEqual(
            asm.sessions[("bench-a", "boot-7")].state, SESSION_SUPERSEDED)
        # 旧 boot 的晚到片进入的是被取代的会话，组保持放弃。
        late = asm.handle_frame(*frame_of(old, 1))
        self.assertEqual(late.status, ABANDONED)

    def test_reopen_allows_same_boot_after_manual_confirmation(self):
        clock = FakeClock()
        asm, _ = build_assembler(clock, expected_boot="boot-7")
        good = make_fixture(sequence=0)
        asm.handle_frame(*frame_of(good, 0))
        far = make_fixture(sequence=32768)
        asm.handle_frame(*frame_of(far, 0))  # 会话失败
        self.assertTrue(asm.reopen_boot("bench-a", "boot-7"))
        again = asm.handle_frame(*frame_of(good, 0))
        self.assertTrue(again.accepted)


class RefetchWindowTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.asm, _ = build_assembler(
            self.clock, retention_groups=3, max_groups=64)

    def _partial(self, sequence):
        fixture = make_fixture(sequence=sequence)
        self.asm.handle_frame(*frame_of(fixture, 0))

    def test_only_groups_inside_retention_window_are_due(self):
        for sequence in range(5):
            self._partial(sequence)
        due = {g.sequence for g, _ in self.asm.due_refetches()}
        # 高水位 4，暂存窗口 {4,3,2}；0、1 不再请求，等 TTL 淘汰。
        self.assertEqual(due, {2, 3, 4})

    def test_complete_and_dead_groups_are_not_due(self):
        fixture = make_fixture(sequence=0)
        self.asm.handle_frame(*frame_of(fixture, 0))
        self.asm.handle_frame(*frame_of(fixture, 1))
        self.asm.handle_frame(*frame_of(fixture, 2))
        self.assertEqual(self.asm.due_refetches(), [])


if __name__ == "__main__":
    unittest.main()

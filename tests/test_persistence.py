"""持久化边界测试：三种组状态的落盘、恢复与晚到片不可翻案。"""

import tempfile
import unittest
from pathlib import Path

from assembler import (
    ABANDONED, ASSEMBLING, PUBLISHED, Assembler, DevicePolicy,
)
from persistence import FileStore
from publisher import FileSink, MemorySink
from tests.test_assembler import FakeClock, frame_of, make_fixture


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self.clock = FakeClock()

    def tearDown(self):
        self.tmp.cleanup()

    def _assembler(self):
        return Assembler(
            {"bench-a": DevicePolicy(group_ttl=100.0)}, clock=self.clock)

    def test_three_statuses_round_trip(self):
        asm = self._assembler()
        # 未组齐：只发两片。
        partial = make_fixture(sequence=1, parts=("a", "b", "c"))
        asm.handle_frame(*frame_of(partial, 0))
        partial_result = asm.handle_frame(*frame_of(partial, 1))
        # 已发布：单片组。
        done = make_fixture(sequence=2, parts=("z",))
        asm.handle_frame(*frame_of(done, 0))
        # 已放弃：同片内容冲突（分片自身摘要自洽，但与首片内容不同）。
        bad = make_fixture(sequence=3)
        asm.handle_frame(*frame_of(bad, 0))
        frame, _ = frame_of(bad, 0)
        import base64
        import hashlib
        evil = b"EVIL"
        frame["payload"] = base64.b64encode(evil).decode()
        frame["part_sha256"] = hashlib.sha256(evil).hexdigest()
        asm.handle_frame(frame, evil)

        store = FileStore(self.state_dir)
        for group in asm.groups.values():
            store.save_group(group)
        store.save_sessions(asm.sessions, asm.active_session)

        # 全新进程：另起组装器从磁盘恢复。
        restored = self._assembler()
        counts = store.restore_into(restored)
        self.assertEqual(counts[ASSEMBLING], 1)
        self.assertEqual(counts[PUBLISHED], 1)
        self.assertEqual(counts[ABANDONED], 1)

        # 晚到片不能把已放弃的组变成成功。
        late = restored.handle_frame(*frame_of(bad, 1))
        self.assertEqual(late.status, ABANDONED)
        self.assertIsNone(late.publish)

        # 未组齐的组可以继续补齐并发布。
        finished = restored.handle_frame(*frame_of(partial, 2))
        self.assertEqual(finished.status, PUBLISHED)

    def test_published_record_is_idempotent_across_restart(self):
        store = FileStore(self.state_dir)
        sink = FileSink(store)
        asm = self._assembler()
        done = make_fixture(sequence=5, parts=("hello",))
        result = asm.handle_frame(*frame_of(done, 0))
        self.assertTrue(sink.deliver(result.publish))

        # 模拟重启：新 Sink 从记录目录初始化。
        sink2 = FileSink(FileStore(self.state_dir))
        self.assertFalse(sink2.deliver(result.publish))  # 不重复交付
        records = store.list_records()
        self.assertEqual(len(records), 1)

    def test_complete_but_undelivered_group_is_published_after_restore(self):
        # 模拟崩溃窗口：磁盘上是 assembling 但片已齐全（发布前崩溃）。
        store = FileStore(self.state_dir)
        asm = self._assembler()
        fixture = make_fixture(sequence=9, parts=("a", "b"))
        asm.handle_frame(*frame_of(fixture, 0))
        group = asm.handle_frame(*frame_of(fixture, 1)).group
        # 人为回退状态，模拟“组齐后、状态翻转为 published 前”落盘。
        group.status = ASSEMBLING
        store.save_group(group)

        from service import CollectorService
        from publisher import MemorySink
        sink = MemorySink()
        service = CollectorService(
            {"devices": {}}, store, sink, clock=self.clock)
        service.restore()
        self.assertEqual(len(service.pending_publish), 1)
        service.tick()  # 周期任务交付遗留完整组
        self.assertEqual(len(sink.delivered_keys()), 1)
        self.assertEqual(sink.records()[0]["sequence"], 9)

    def test_memory_sink_idempotency_and_payload(self):
        sink = MemorySink()
        asm = self._assembler()
        fixture = make_fixture(sequence=7, parts=("温度,", "18.2\n"))
        result = asm.handle_frame(*frame_of(fixture, 0))
        asm.handle_frame(*frame_of(fixture, 1))
        group = asm.groups[("bench-a", "boot-7", 7)]
        self.assertTrue(sink.deliver(group))
        self.assertFalse(sink.deliver(group))
        record = sink.records()[0]
        self.assertEqual(record["length"], len("温度,18.2\n".encode()))
        self.assertEqual(len(sink.delivered_keys()), 1)


if __name__ == "__main__":
    unittest.main()

"""缺片重取的上界测试：尝试次数、暂存失效与总时长淘汰。"""

import unittest

from assembler import Group
from retrieval import MemoryRetransmit, Retriever
from source import Retention
from tests.test_assembler import FakeClock, make_fixture


class StubRetention:
    def __init__(self, packets_left=2):
        self.packets_left = packets_left
        self.calls = 0

    def missing(self, device, boot, sequence, indexes):
        self.calls += 1
        if self.packets_left <= 0:
            return []
        self.packets_left -= 1
        return [b"{}"]  # 内容由服务层再解码，本测试只看次数与耗尽


class RetrieverTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.fixture = make_fixture(sequence=10)

    def _group(self):
        return Group(device="bench-a", boot="boot-7", sequence=10,
                     count=3, group_sha256="g" * 64)

    def test_attempts_are_bounded(self):
        retention = StubRetention(packets_left=99)
        retriever = Retriever(MemoryRetransmit(retention),
                              max_attempts=3, clock=self.clock)
        group = self._group()
        for _ in range(3):
            self.assertEqual(retriever.fetch(group, [1]), [b"{}"])
        # 第 4 次不再触达源端。
        self.assertEqual(retriever.fetch(group, [1]), [])
        self.assertEqual(retention.calls, 3)
        self.assertEqual(retriever.status(group.key)["attempts"], 3)

    def test_empty_response_marks_exhausted(self):
        retention = StubRetention(packets_left=0)
        retriever = Retriever(MemoryRetransmit(retention),
                              max_attempts=5, clock=self.clock)
        group = self._group()
        self.assertEqual(retriever.fetch(group, [1]), [])
        self.assertTrue(retriever.status(group.key)["exhausted"])
        # 已耗尽后即使时间流逝也不再请求，组只能等 TTL 淘汰。
        self.clock.advance(100)
        self.assertEqual(retriever.fetch(group, [1]), [])
        self.assertEqual(retention.calls, 1)

    def test_total_timeout_bounds_retry(self):
        retention = StubRetention(packets_left=99)
        retriever = Retriever(MemoryRetransmit(retention),
                              max_attempts=10, timeout=5.0,
                              clock=self.clock)
        group = self._group()
        self.assertEqual(retriever.fetch(group, [1]), [b"{}"])
        self.clock.advance(6.0)
        self.assertEqual(retriever.fetch(group, [1]), [])
        self.assertTrue(retriever.status(group.key)["exhausted"])

    def test_real_retention_boundary(self):
        # 与 source.Retention 的契约对齐：身份不符/失效返回空。
        source = Retention(self.fixture)
        retriever = Retriever(MemoryRetransmit(source),
                              max_attempts=3, clock=self.clock)
        group = self._group()
        packets = retriever.fetch(group, [0, 2])
        self.assertEqual(len(packets), 2)
        source.expired = True
        self.assertEqual(retriever.fetch(group, [1]), [])
        self.assertTrue(retriever.status(group.key)["exhausted"])

    def test_forget_clears_tracking(self):
        retention = StubRetention()
        retriever = Retriever(MemoryRetransmit(retention),
                              max_attempts=1, clock=self.clock)
        group = self._group()
        retriever.fetch(group, [1])
        retriever.forget(group.key)
        self.assertEqual(retriever.status(group.key)["attempts"], 0)


if __name__ == "__main__":
    unittest.main()

"""协议层结构校验测试：坏帧在进入组装器之前被拒绝。"""

import json
import unittest

from protocol import (
    HARD_MAX_COUNT, ProtocolError, decode_frame,
    decode_missing_request, encode_missing_request, is_status_query,
)
from tests.test_assembler import make_fixture
from source import packets


class ProtocolTests(unittest.TestCase):
    def _frame(self, **overrides):
        frame = json.loads(packets(make_fixture())[0])
        frame.update(overrides)
        return json.dumps(frame).encode()

    def test_valid_frame_decodes(self):
        fixture = make_fixture(parts=("温度,", "18.2\n"))
        frame, payload = decode_frame(packets(fixture)[0])
        self.assertEqual(frame["device"], "bench-a")
        self.assertEqual(payload, "温度,".encode())

    def test_bad_json_rejected(self):
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(b"not-json")
        self.assertEqual(ctx.exception.reason, "bad_json")

    def test_missing_field_rejected(self):
        raw = self._frame()
        frame = json.loads(raw)
        del frame["boot"]
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(json.dumps(frame).encode())
        self.assertEqual(ctx.exception.reason, "missing_field")

    def test_bad_base64_rejected(self):
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(self._frame(payload="@@@"))
        self.assertEqual(ctx.exception.reason, "bad_base64")

    def test_digest_mismatch_rejected_at_boundary(self):
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(self._frame(part_sha256="f" * 64))
        self.assertEqual(ctx.exception.reason, "digest_mismatch")

    def test_oversized_count_rejected(self):
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(self._frame(count=HARD_MAX_COUNT + 1, index=0))
        self.assertEqual(ctx.exception.reason, "oversized_group")

    def test_index_out_of_range_rejected(self):
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(self._frame(index=3, count=3))
        self.assertEqual(ctx.exception.reason, "bad_field")

    def test_sequence_range_rejected(self):
        with self.assertRaises(ProtocolError):
            decode_frame(self._frame(sequence=65536))

    def test_missing_request_roundtrip(self):
        raw = encode_missing_request("bench-a", "boot-7", 10, [1, 2])
        request = decode_missing_request(raw)
        self.assertEqual(request["indexes"], [1, 2])
        with self.assertRaises(ProtocolError):
            decode_missing_request(b'{"request":"missing","indexes":[]}')

    def test_status_query_detection(self):
        self.assertTrue(is_status_query(b'{"query":"status"}'))
        self.assertFalse(is_status_query(b'{"query":"other"}'))
        self.assertFalse(is_status_query(b"garbage"))


if __name__ == "__main__":
    unittest.main()

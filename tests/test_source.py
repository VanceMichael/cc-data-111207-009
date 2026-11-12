import json
import unittest
from pathlib import Path
from reader import inspect
from source import packets


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).parents[1] / "fixtures/source.json").read_text(encoding="utf-8"))
        self.frames = []

    def test_packet_hash(self):
        from source import packets
        from reader import decode_packet
        frame,payload=decode_packet(packets(self.fixture)[0])
        self.assertEqual(payload,'温度,'.encode())

    def test_corrupt_packet_rejected(self):
        from source import packets
        from reader import decode_packet
        frame=json.loads(packets(self.fixture)[0])
        frame['payload']='YQ=='
        with self.assertRaises(ValueError):decode_packet(json.dumps(frame).encode())

    def test_real_udp_roundtrip(self):
        import socket
        from source import packets,send_udp
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as channel:
            channel.bind(('127.0.0.1',0))
            channel.settimeout(1)
            frames=packets(self.fixture)
            origin=send_udp(channel.getsockname(),[frames[2],frames[0]])
            received,address=channel.recvfrom(65535)
            self.assertEqual(received,frames[2])
            self.assertEqual(address,origin)

    def test_expired_source_has_no_retransmit(self):
        from source import Retention
        source=Retention(self.fixture)
        self.assertEqual(len(source.missing('bench-a','boot-7',65535,[1])),1)
        source.expired=True
        self.assertEqual(source.missing('bench-a','boot-7',65535,[1]),[])


if __name__ == "__main__":
    unittest.main()

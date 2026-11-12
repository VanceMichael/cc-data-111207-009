"""本机合成设备：通过真实回环 UDP 发送测量分片并应答缺片重取。

不连接任何真实仪器。测量定义来自 JSON（如 fixtures/scenarios.json），
与 fixtures/source.json 同结构；本工具复用 source.packets 的帧格式。

场景开关：
--drop INDEX[,INDEX]   首发时丢弃这些片号（靠重取补回）
--shuffle              乱序发送
--dup INDEX            首发后再重传一次同片（验证幂等）
--expire               让重取暂存立即失效（missing 返回空）
--serve SECONDS        发完后监听 missing 请求的秒数
--new-boot BOOT        再发一轮并使用新启动代次（设备重启）
"""

import argparse
import json
import socket
import time
from pathlib import Path

from protocol import decode_missing_request, ProtocolError
from source import packets

MAX_DATAGRAM = 65535


class SyntheticDevice:
    def __init__(self, bind_addr=("127.0.0.1", 0), logger=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(bind_addr)
        self.socket.setblocking(False)
        # 暂存区：(device, boot, sequence) -> 全部帧；只保留最近若干组。
        self.retention = {}
        self.retention_order = []
        self.expired_identities = set()
        self.logger = logger or (lambda text: None)

    @property
    def address(self):
        return self.socket.getsockname()

    def close(self):
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    def stage(self, measurement, retention_groups):
        frames = packets(measurement)
        identity = (measurement["device"], measurement["boot"],
                    measurement["sequence"])
        self.retention[identity] = frames
        self.retention_order.append(identity)
        for stale in self.retention_order[:-retention_groups]:
            self.retention.pop(stale, None)
        self.retention_order = self.retention_order[-retention_groups:]
        return frames

    def send_measurement(self, destination, measurement, retention_groups=16,
                         drop=(), shuffle=False, dup=()):
        frames = self.stage(measurement, retention_groups)
        order = list(range(len(frames)))
        if shuffle:
            order.reverse()  # 确定性乱序：倒序发送
        chosen = [i for i in order if i not in set(drop)]
        for index in chosen:
            self.socket.sendto(frames[index], destination)
        for index in dup:
            if 0 <= index < len(frames):
                self.socket.sendto(frames[index], destination)
        self.logger("sent %s seq=%s 片序=%s 丢弃=%s"
                    % (measurement["device"], measurement["sequence"],
                       chosen, list(drop)))
        return frames

    def expire(self, measurement):
        identity = (measurement["device"], measurement["boot"],
                    measurement["sequence"])
        self.expired_identities.add(identity)

    # ------------------------------------------------------------------
    def serve(self, seconds):
        """在 seconds 内监听并应答 missing 请求。"""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                data, address = self.socket.recvfrom(MAX_DATAGRAM)
            except BlockingIOError:
                time.sleep(0.02)
                continue
            except OSError:
                break
            try:
                request = decode_missing_request(data)
            except ProtocolError as exc:
                self.logger("忽略坏请求: %s" % exc.reason)
                continue
            identity = (request["device"], request["boot"],
                        request["sequence"])
            if identity in self.expired_identities:
                self.logger("暂存失效 %s，返回空" % (identity,))
                continue  # 空应答语义：不回任何帧
            frames = self.retention.get(identity)
            if frames is None:
                self.logger("未知组 %s，返回空" % (identity,))
                continue
            for index in request["indexes"]:
                if 0 <= index < len(frames):
                    self.socket.sendto(frames[index], address)
            self.logger("重取 %s 片=%s -> %s"
                        % (identity, request["indexes"], address))

    def send_raw(self, raw, destination):
        """供冲突/坏帧测试使用的逃生口。"""
        self.socket.sendto(raw, destination)


def load_measurements(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    return [data]


def main(argv=None):
    parser = argparse.ArgumentParser(description="本机合成 UDP 设备")
    parser.add_argument("--fixture", type=Path,
                        default=Path(__file__).parent / "fixtures/scenarios.json")
    parser.add_argument("--target", required=True,
                        help="采集端数据口 host:port")
    parser.add_argument("--bind", default="127.0.0.1:0")
    parser.add_argument("--drop", default="")
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--dup", default="")
    parser.add_argument("--expire", action="store_true")
    parser.add_argument("--serve", type=float, default=3.0)
    parser.add_argument("--new-boot", default=None)
    args = parser.parse_args(argv)

    host, port = args.target.split(":")
    target = (host, int(port))
    bind_host, bind_port = args.bind.split(":")
    drop = tuple(int(x) for x in args.drop.split(",") if x)
    dup = tuple(int(x) for x in args.dup.split(",") if x)
    measurements = load_measurements(args.fixture)

    with SyntheticDevice((bind_host, int(bind_port)), print) as device:
        print("[device] 绑定来源地址 %s" % (device.address,))
        first = measurements[0]
        device.send_measurement(target, first,
                                retention_groups=first.get("retention_groups", 16),
                                drop=drop, shuffle=args.shuffle, dup=dup)
        if args.expire:
            device.expire(first)
        if args.new_boot and len(measurements) > 1:
            time.sleep(0.3)
            restarted = dict(measurements[1], boot=args.new_boot)
            device.send_measurement(target, restarted)
        device.serve(args.serve)


if __name__ == "__main__":
    main()

"""有上界的缺片重取。

Retriever 不自己决定哪些组该重取（那是 assembler.due_refetches 的职责，
受 retention_groups 暂存窗口约束），只负责：
- 向设备暂存边界发请求并收回片；
- 用 attempts 上限和截止时间保证离线设备不会被无限重试；
- 设备暂存失效（返回空）时记录耗尽，等 TTL 淘汰，而不是永久占缓存。

RetransmitSource 是可注入边界：测试用内存实现，生产用 UDP 实现，
与 source.Retention.missing 的语义一致。
"""

from dataclasses import dataclass

from protocol import ProtocolError, decode_frame, encode_missing_request


class RetransmitSource:
    def request_missing(self, device, boot, sequence, indexes):  # pragma: no cover
        """返回可直接送入协议层的数据报字节列表；暂存失效返回 []。"""
        raise NotImplementedError


@dataclass
class _Attempt:
    attempts: int = 0
    exhausted: bool = False          # 设备明确返回空，暂存已失效
    last_error: str | None = None


class Retriever:
    def __init__(self, source, max_attempts=3, timeout=None, clock=None):
        self.source = source
        self.max_attempts = max_attempts
        self.timeout = timeout        # 每组从重取开始起的总时长上界（秒）
        self._track = {}              # group_key -> _Attempt
        self._start_at = {}           # group_key -> 首次请求时刻
        self.clock = clock or _system_clock

    def fetch(self, group, indexes):
        """返回取回的数据报 (frame_bytes 列表)；超过上界或已耗尽返回 []。"""
        key = group.key
        now = self.clock()
        track = self._track.setdefault(key, _Attempt())
        if track.exhausted or track.attempts >= self.max_attempts:
            return []
        if self.timeout is not None:
            started = self._start_at.setdefault(key, now)
            if now - started > self.timeout:
                track.exhausted = True
                return []
        track.attempts += 1
        try:
            packets = self.source.request_missing(
                group.device, group.boot, group.sequence, list(indexes))
        except ProtocolError as exc:
            track.last_error = exc.reason
            return []
        except OSError as exc:
            track.last_error = "socket_error:%s" % exc.__class__.__name__
            return []
        if not packets:
            # 源端暂存失效：有界重试到此为止，组交给 TTL 淘汰。
            track.exhausted = True
            return []
        return packets

    def status(self, key):
        track = self._track.get(key)
        if track is None:
            return {"attempts": 0, "exhausted": False}
        return {"attempts": track.attempts, "exhausted": track.exhausted,
                "last_error": track.last_error}

    def tracked_keys(self):
        return list(self._track)

    def forget(self, key):
        self._track.pop(key, None)
        self._start_at.pop(key, None)


class MemoryRetransmit(RetransmitSource):
    """测试边界：直接复用 source.Retention 风格的内存暂存。

    bind_address 是设备的报文来源地址，重取回来的帧同样要过来源校验。
    """

    def __init__(self, retention, bind_address=None):
        self.retention = retention
        self.bind_address = bind_address

    def request_missing(self, device, boot, sequence, indexes):
        return self.retention.missing(device, boot, sequence, indexes)


class UdpRetransmit(RetransmitSource):
    """真实 UDP 边界：发 missing 请求到设备，收一轮回应。

    用独立的临时 socket 收发，回应地址必须等于目标地址，防止第三方
    注入重取报文。
    """

    def __init__(self, destination, wait=0.2, max_datagram=65535):
        self.destination = destination
        self.wait = wait
        self.max_datagram = max_datagram
        self.bind_address = destination

    def request_missing(self, device, boot, sequence, indexes):
        import socket
        request = encode_missing_request(device, boot, sequence, indexes)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
            channel.settimeout(self.wait)
            channel.sendto(request, self.destination)
            packets = []
            while True:
                try:
                    data, address = channel.recvfrom(self.max_datagram)
                except socket.timeout:
                    break
                if address != self.destination:
                    continue
                # 结构校验在边界完成；坏回应直接丢弃，不污染组装器。
                try:
                    decode_frame(data)
                except ProtocolError:
                    continue
                packets.append(data)
                if len(packets) >= len(indexes):
                    break
            return packets


def _system_clock():
    import time
    return time.monotonic()

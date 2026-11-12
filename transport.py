"""网络接收边界：真实回环 UDP 的接收与状态查询。

Transport 只负责 socket 生命周期与地址记录，不解释业务；
解释失败（ProtocolError）由服务层计数隔离。控制口提供 JSON 状态查询，
与数据口分开，避免查询流量进入组装路径。
"""

import json
import socket

from protocol import MAX_DATAGRAM, decode_control, is_status_query


class UdpTransport:
    def __init__(self, data_addr, control_addr=None):
        self.data_addr = data_addr
        self.control_addr = control_addr
        self.data_socket = None
        self.control_socket = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *exc):
        self.close()

    def open(self):
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.data_socket.bind(self.data_addr)
        self.data_socket.setblocking(False)
        if self.control_addr is not None:
            self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.control_socket.setsockopt(socket.SOL_SOCKET,
                                           socket.SO_REUSEADDR, 1)
            self.control_socket.bind(self.control_addr)
            self.control_socket.setblocking(False)

    def close(self):
        for sock in (self.data_socket, self.control_socket):
            if sock is not None:
                sock.close()
        self.data_socket = None
        self.control_socket = None

    @property
    def bound_data_addr(self):
        return self.data_socket.getsockname()

    @property
    def bound_control_addr(self):
        if self.control_socket is None:
            return None
        return self.control_socket.getsockname()

    def poll(self):
        """非阻塞排空两个 socket，返回 (数据报, 控制请求) 两个列表。"""
        datagrams, controls = [], []
        self._drain(self.data_socket, datagrams, MAX_DATAGRAM)
        if self.control_socket is not None:
            self._drain(self.control_socket, controls, MAX_DATAGRAM)
        return datagrams, controls

    @staticmethod
    def _drain(sock, bucket, size):
        while True:
            try:
                data, address = sock.recvfrom(size)
            except BlockingIOError:
                return
            except OSError:
                return
            bucket.append((data, address))

    def reply_status(self, address, snapshot):
        if self.control_socket is None:
            return
        self.control_socket.sendto(
            json.dumps(snapshot, ensure_ascii=False).encode("utf-8"), address)

    def send(self, data, address):
        self.data_socket.sendto(data, address)


def send_status_query(control_addr, wait=1.0):
    """客户端辅助：向控制口发查询并等待 JSON 应答。"""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.settimeout(wait)
        channel.bind(("127.0.0.1", 0))
        channel.sendto(json.dumps({"query": "status"}).encode("utf-8"),
                       control_addr)
        data, _ = channel.recvfrom(MAX_DATAGRAM)
        return json.loads(data.decode("utf-8"))


def looks_like_status(raw):
    return is_status_query(raw)


def parse_control(raw):
    return decode_control(raw)

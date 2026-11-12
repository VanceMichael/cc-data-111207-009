"""线路协议：UDP 数据报的编解码与结构校验。

本模块只处理字节与报文结构，不持有任何组装状态，也不做 I/O，
以便网络边界与组装状态机分别测试。
"""

import base64
import hashlib
import json

# UDP 载荷理论上限，接收缓冲按此尺寸开设。
MAX_DATAGRAM = 65535
# 单组总片数的硬上限，防止异常 count 占用资源；可被配置进一步收紧。
HARD_MAX_COUNT = 1024
SEQUENCE_MOD = 65536

FRAME_FIELDS = (
    "device", "boot", "sequence", "index", "count",
    "part_sha256", "group_sha256", "payload",
)
_HEX_DIGITS = set("0123456789abcdef")


class ProtocolError(ValueError):
    """报文无法被解释为合法帧；reason 供计数器与状态查询分类。"""

    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def _require(condition, reason, message):
    if not condition:
        raise ProtocolError(reason, message)


def _is_sha256_hex(value):
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(ch in _HEX_DIGITS for ch in value)
    )


def group_key(device, boot, sequence):
    """一次测量的身份：设备 + 启动代次 + 代次内 16 位序号。"""
    return device, boot, sequence


def decode_frame(raw, max_count=HARD_MAX_COUNT):
    """把一个 UDP 数据报解码为 (frame, payload)。

    结构、取值范围、Base64、分片摘要任一不符都抛出 ProtocolError，
    调用方不得把这类数据报送入组装状态机。
    """
    _require(isinstance(raw, (bytes, bytearray)), "bad_datagram", "数据报不是字节串")
    try:
        frame = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("bad_json", "数据报不是合法 UTF-8 JSON") from exc
    _require(isinstance(frame, dict), "bad_frame", "报文顶层不是对象")

    missing = [name for name in FRAME_FIELDS if name not in frame]
    _require(not missing, "missing_field", "报文缺少字段: %s" % ",".join(missing))

    _require(isinstance(frame["device"], str) and frame["device"],
             "bad_field", "device 必须是非空字符串")
    _require(isinstance(frame["boot"], str) and frame["boot"],
             "bad_field", "boot 必须是非空字符串")
    sequence = frame["sequence"]
    _require(isinstance(sequence, int) and not isinstance(sequence, bool)
             and 0 <= sequence < SEQUENCE_MOD,
             "bad_field", "sequence 必须是 0..65535 的整数")
    index = frame["index"]
    count = frame["count"]
    _require(isinstance(index, int) and not isinstance(index, bool) and index >= 0,
             "bad_field", "index 必须是非负整数")
    _require(isinstance(count, int) and not isinstance(count, bool) and count > 0,
             "bad_field", "count 必须是正整数")
    _require(index < count, "bad_field", "index 必须小于 count")
    _require(count <= max_count, "oversized_group",
             "总片数 %d 超过上限 %d" % (count, max_count))
    _require(_is_sha256_hex(frame["part_sha256"]), "bad_field", "part_sha256 格式非法")
    _require(_is_sha256_hex(frame["group_sha256"]), "bad_field", "group_sha256 格式非法")

    try:
        payload = base64.b64decode(frame["payload"], validate=True)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("bad_base64", "payload 不是合法 Base64") from exc

    actual = hashlib.sha256(payload).hexdigest()
    _require(actual == frame["part_sha256"], "digest_mismatch", "分片摘要不符")
    return frame, payload


def encode_missing_request(device, boot, sequence, indexes):
    """缺片重取请求：采集端 -> 设备暂存边界。"""
    return json.dumps({
        "request": "missing",
        "device": device,
        "boot": boot,
        "sequence": sequence,
        "indexes": list(indexes),
    }, separators=(",", ":")).encode("utf-8")


def decode_missing_request(raw):
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("bad_json", "重取请求不是合法 JSON") from exc
    _require(isinstance(obj, dict) and obj.get("request") == "missing",
             "bad_request", "不是 missing 请求")
    for name in ("device", "boot", "sequence", "indexes"):
        _require(name in obj, "missing_field", "重取请求缺少 %s" % name)
    _require(isinstance(obj["indexes"], list) and obj["indexes"],
             "bad_request", "indexes 必须是非空列表")
    _require(all(isinstance(i, int) and not isinstance(i, bool) and i >= 0
                 for i in obj["indexes"]),
             "bad_request", "indexes 必须是非负整数")
    _require(isinstance(obj["sequence"], int) and 0 <= obj["sequence"] < SEQUENCE_MOD,
             "bad_request", "sequence 非法")
    return obj


def is_status_query(raw):
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(obj, dict) and obj.get("query") == "status"


def decode_control(raw):
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("bad_json", "控制报文不是合法 JSON") from exc
    _require(isinstance(obj, dict), "bad_request", "控制报文顶层不是对象")
    return obj

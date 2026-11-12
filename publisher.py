"""业务发布边界：把完整组交付给下游计算程序。

Sink 只有一个 deliver 方法且必须幂等——同一组重复收齐只发布一次
（assembler 只在状态跃迁为 PUBLISHED 时触发，Sink 再以身份键兜底）。
内存实现用于测试，FileSink 落 JSON 记录并可被现有 Python 代码直接读取。
"""

import json
from pathlib import Path

from persistence import serialize_record


class Sink:
    def deliver(self, group):  # pragma: no cover - 接口
        raise NotImplementedError

    def delivered_keys(self):  # pragma: no cover
        raise NotImplementedError


class MemorySink(Sink):
    def __init__(self):
        self._records = {}

    def deliver(self, group):
        if group.key in self._records:
            return False
        self._records[group.key] = serialize_record(group)
        return True

    def delivered_keys(self):
        return sorted(self._records)

    def records(self):
        return [self._records[key] for key in sorted(self._records)]


class FileSink(Sink):
    """薄封装：真正的原子写在 persistence.FileStore 中，
    发布路径与状态路径共用同一份记录契约。"""

    def __init__(self, store):
        self.store = store
        self._keys = {self._key_from_path(p) for p in store.list_records()}

    @staticmethod
    def _key_from_path(path):
        stem = path.stem  # <device>__<boot>__<seq>
        device, boot, seq = stem.split("__", 2)
        return device, boot, int(seq)

    def deliver(self, group):
        if group.key in self._keys:
            return False
        self.store.save_group(group)
        self._keys.add(group.key)
        return True

    def delivered_keys(self):
        return sorted(self._keys)


def load_record(path):
    """供计算程序/查看器读取发布记录的辅助函数。"""
    return json.loads(Path(path).read_text(encoding="utf-8"))

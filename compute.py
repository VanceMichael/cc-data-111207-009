"""下游计算程序示例：只消费已发布的完整记录，不接触半成品组。

采集服务保证 records/ 下的每条记录都是全部片齐全且整组摘要一致的
完整负载；本程序按记录的 group_sha256 复核后再计算，绝不猜测缺失部分。

    python3 compute.py --state-dir state
或直接指定记录文件：
    python3 compute.py state/records/bench-a/*.json
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path


def load_verified(record_path):
    record = json.loads(Path(record_path).read_text(encoding="utf-8"))
    payload = base64.b64decode(record["payload"], validate=True)
    actual = hashlib.sha256(payload).hexdigest()
    if actual != record["group_sha256"]:
        raise ValueError("记录摘要不符，拒绝计算: %s" % record_path)
    if len(payload) != record["length"]:
        raise ValueError("记录长度不符，拒绝计算: %s" % record_path)
    return record, payload


def compute(payload):
    """占位计算：真实计算程序在此替换，输入保证是完整测量。"""
    text = payload.decode("utf-8")
    lines = [line for line in text.splitlines() if line]
    return {"行数": len(lines), "字节数": len(payload), "预览": lines[:3]}


def iter_record_paths(state_dir, explicit):
    if explicit:
        yield from (Path(p) for p in explicit)
        return
    records = Path(state_dir) / "records"
    if records.exists():
        yield from sorted(records.rglob("*.json"))


def main(argv=None):
    parser = argparse.ArgumentParser(description="发布记录的下游计算示例")
    parser.add_argument("--state-dir", default="state")
    parser.add_argument("records", nargs="*", help="显式记录文件路径")
    args = parser.parse_args(argv)

    found = False
    for path in iter_record_paths(args.state_dir, args.records):
        found = True
        record, payload = load_verified(path)
        result = compute(payload)
        print(json.dumps({
            "来源": "%s/%s#%s" % (record["device"], record["boot"],
                                  record["sequence"]),
            "计算": result,
        }, ensure_ascii=False))
    if not found:
        print("# 没有可用的发布记录")


if __name__ == "__main__":
    main()

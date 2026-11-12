"""持久化边界：组装状态的磁盘表示与进程恢复。

目录布局（state_dir 由调用方显式传入，默认 state/ 已在 .gitignore）：

    groups/<device>/<boot>__<seq>.json   每组一份，含全部已到片负载
    records/<device>/<boot>__<seq>.json  发布记录（完整负载，交给计算程序）
    sessions.json                        启动代次会话状态
    marker                               启动标记，原子写用于观察崩溃恢复

组状态随每次变更原子覆盖写；恢复时三种状态分别归位，晚到片对
ABANDONED/PUBLISHED 组无效这一规则由 assembler 保证，持久层只忠实重建。
"""

import base64
import json
import os
import tempfile
from pathlib import Path

from assembler import (
    ABANDONED, ASSEMBLING, PUBLISHED, Group, Part, Session,
)


def _safe_name(device, boot, sequence):
    # 身份字段仅作文件名用途，替换路径分隔符避免越界写。
    cleaned = lambda value: str(value).replace("/", "_").replace(os.sep, "_")
    return "%s__%s" % (cleaned(device), cleaned(boot)), "%05d" % sequence


def _atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _group_paths(state_dir, group):
    head, seq = _safe_name(group.device, group.boot, group.sequence)
    return (Path(state_dir) / "groups" / group.device / f"{head}__{seq}.json",
            Path(state_dir) / "records" / group.device / f"{head}__{seq}.json")


def serialize_group(group):
    return {
        "device": group.device,
        "boot": group.boot,
        "sequence": group.sequence,
        "count": group.count,
        "group_sha256": group.group_sha256,
        "status": group.status,
        "abandon_reason": group.abandon_reason,
        "created_at": group.created_at,
        "updated_at": group.updated_at,
        "deadline": group.deadline,
        "parts": [
            {"index": part.index,
             "part_sha256": part.part_sha256,
             "payload": base64.b64encode(part.payload).decode("ascii")}
            for part in sorted(group.parts.values(), key=lambda p: p.index)
        ],
    }


def deserialize_group(data):
    group = Group(
        device=data["device"], boot=data["boot"], sequence=data["sequence"],
        count=data["count"], group_sha256=data["group_sha256"],
        status=data["status"], abandon_reason=data.get("abandon_reason"),
        created_at=data["created_at"], updated_at=data["updated_at"],
        deadline=data["deadline"],
    )
    for item in data["parts"]:
        group.parts[item["index"]] = Part(
            index=item["index"], part_sha256=item["part_sha256"],
            payload=base64.b64decode(item["payload"], validate=True),
        )
    return group


def serialize_record(group):
    """发布记录：计算程序只需要完整负载与可核对的摘要。"""
    payload = group.payload_bytes()
    return {
        "device": group.device,
        "boot": group.boot,
        "sequence": group.sequence,
        "group_sha256": group.group_sha256,
        "length": len(payload),
        "payload": base64.b64encode(payload).decode("ascii"),
    }


class FileStore:
    def __init__(self, state_dir):
        self.state_dir = Path(state_dir)
        self.groups_dir = self.state_dir / "groups"
        self.records_dir = self.state_dir / "records"
        self.sessions_path = self.state_dir / "sessions.json"

    # ------------------------------------------------------------------
    def save_group(self, group):
        group_path, record_path = _group_paths(self.state_dir, group)
        _atomic_write(group_path,
                      json.dumps(serialize_group(group), ensure_ascii=False)
                      .encode("utf-8"))
        if group.status == PUBLISHED:
            # 记录与组状态分目录，记录是交付给下游的契约。
            _atomic_write(record_path,
                          json.dumps(serialize_record(group), ensure_ascii=False)
                          .encode("utf-8"))

    def save_sessions(self, sessions, active_session):
        payload = {
            "sessions": [
                {"device": s.device, "boot": s.boot, "state": s.state,
                 "high_sequence": s.high_sequence}
                for s in sessions.values()
            ],
            "active": {
                device: [session.device, session.boot]
                for device, session in active_session.items()
            },
        }
        _atomic_write(self.sessions_path,
                      json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    # ------------------------------------------------------------------
    def load(self):
        """返回 (groups, sessions, active_session)，供 assembler 恢复。"""
        groups = []
        if self.groups_dir.exists():
            for path in sorted(self.groups_dir.rglob("*.json")):
                with open(path, "rb") as handle:
                    groups.append(deserialize_group(json.loads(handle.read())))

        sessions = {}
        active_session = {}
        if self.sessions_path.exists():
            payload = json.loads(self.sessions_path.read_text(encoding="utf-8"))
            for item in payload["sessions"]:
                session = Session(item["device"], item["boot"],
                                  state=item["state"],
                                  high_sequence=item["high_sequence"])
                sessions[(item["device"], item["boot"])] = session
            for device, key in payload.get("active", {}).items():
                if tuple(key) in sessions:
                    active_session[device] = sessions[tuple(key)]
        return groups, sessions, active_session

    def restore_into(self, assembler):
        """把磁盘状态灌入组装器；返回各状态计数。"""
        groups, sessions, active_session = self.load()
        assembler.sessions.update(sessions)
        assembler.active_session.update(active_session)
        counts = {ASSEMBLING: 0, PUBLISHED: 0, ABANDONED: 0}
        for group in groups:
            assembler.restore_group(group)
            counts[group.status] = counts.get(group.status, 0) + 1
        return counts

    # ------------------------------------------------------------------
    def list_records(self):
        if not self.records_dir.exists():
            return []
        return sorted(self.records_dir.rglob("*.json"))

    def prune_abandoned(self, keep=0):
        """回收已放弃组的缓存（默认全清），记录目录不受影响。"""
        removed = []
        if not self.groups_dir.exists():
            return removed
        for path in sorted(self.groups_dir.rglob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("status") == ABANDONED:
                path.unlink()
                removed.append(path)
        return removed

    def write_marker(self, text):
        _atomic_write(self.state_dir / "marker", text.encode("utf-8"))

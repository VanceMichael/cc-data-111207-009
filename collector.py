"""采集服务命令行入口与离线状态查询。

    python3 collector.py --config config/collector.json
    python3 collector.py --config config/collector.json --status

服务在数据口收 UDP 帧，在控制口回答 JSON 状态查询；状态同时落盘，
--status 可在服务不运行时直接读取磁盘上的组与发布记录。
"""

import argparse
import json
from pathlib import Path

from persistence import FileStore
from publisher import FileSink
from retrieval import UdpRetransmit
from service import CollectorService, load_config
from transport import UdpTransport, send_status_query


def build_service(config, state_dir):
    store = FileStore(state_dir)
    sink = FileSink(store)
    sources = {}
    for name, policy in config.get("devices", {}).items():
        target = policy.get("retransmit_addr") or policy.get("address")
        if target:
            sources[name] = UdpRetransmit(
                tuple(target), wait=config.get("retransmit_wait", 0.2))
    return CollectorService(
        config, store, sink, retransmit_sources=sources,
        logger=lambda event, detail: print(
            "[collector] %s %s" % (event, detail)))


def offline_status(state_dir):
    store = FileStore(state_dir)
    groups, sessions, active = store.load()
    by_status = {"assembling": 0, "published": 0, "abandoned": 0}
    detail = []
    for group in groups:
        by_status[group.status] = by_status.get(group.status, 0) + 1
        detail.append({
            "device": group.device, "boot": group.boot,
            "sequence": group.sequence, "status": group.status,
            "held_parts": len(group.parts), "count": group.count,
            "abandon_reason": group.abandon_reason,
        })
    return {
        "state_dir": str(state_dir),
        "sessions": [
            {"device": s.device, "boot": s.boot, "state": s.state,
             "high_sequence": s.high_sequence}
            for s in sessions.values()],
        "groups": detail,
        "totals": by_status,
        "records": [str(p) for p in store.list_records()],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="多片遥测采集服务")
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).parent / "config/collector.json")
    parser.add_argument("--state-dir", type=Path,
                        default=Path(__file__).parent / "state")
    parser.add_argument("--status", action="store_true",
                        help="查询在线服务；服务未运行时回退到磁盘状态")
    parser.add_argument("--offline", action="store_true",
                        help="只读取磁盘状态，不联系运行中的服务")
    args = parser.parse_args(argv)

    config = load_config(args.config)

    if args.offline:
        print(json.dumps(offline_status(args.state_dir),
                         ensure_ascii=False, indent=2))
        return

    if args.status:
        try:
            snapshot = send_status_query(tuple(config["control_addr"]))
            print(json.dumps(snapshot, ensure_ascii=False, indent=2))
            return
        except OSError:
            print("# 控制口无应答，回退到磁盘状态")
            print(json.dumps(offline_status(args.state_dir),
                             ensure_ascii=False, indent=2))
            return

    service = build_service(config, args.state_dir)
    service.restore()
    transport = UdpTransport(tuple(config["data_addr"]),
                             tuple(config["control_addr"]))
    print("[collector] 数据口 %s 控制口 %s 状态目录 %s"
          % (transport.data_addr and config["data_addr"],
             config["control_addr"], args.state_dir))
    with transport:
        print("[collector] 实际绑定 数据口=%s 控制口=%s"
              % (transport.bound_data_addr, transport.bound_control_addr))
        service.run(transport)


if __name__ == "__main__":
    main()

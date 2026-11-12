"""真实回环 UDP 演示：采集服务 + 合成设备，全程不接真实仪器。

在同一进程内用线程运行采集服务（真实 monotonic 时钟、真实 socket），
主线程驱动合成设备，依次展示：
  1. 65535 -> 0 序号回绕被解释为同一启动代次内的连续测量；
  2. 乱序到达仍组齐发布；
  3. 丢一片后经有上界重取补回；
  4. 设备中途退出，未组齐的组到 TTL 被淘汰，不永久占缓存；
  5. 同片内容冲突被隔离，永远不会发布；
  6. 跨越半个序号空间时代次失败，设备以新 boot 重启后恢复。

    python3 demo.py
"""

import base64
import hashlib
import json
import tempfile
import threading
import time
from pathlib import Path

from assembler import ABANDONED, PUBLISHED
from collector import offline_status
from device import SyntheticDevice
from persistence import FileStore
from publisher import FileSink
from retrieval import MemoryRetransmit
from service import CollectorService
from source import Retention, packets
from transport import UdpTransport

STATE_DIR = Path(tempfile.mkdtemp(prefix="collector-demo-"))


def log(title):
    print("\n=== %s ===" % title)


def start_service():
    transport = UdpTransport(("127.0.0.1", 0), ("127.0.0.1", 0))
    transport.open()
    config = {
        "refetch_interval": 0.2,
        "refetch_max_attempts": 4,
        "sweep_interval": 0.2,
        "devices": {"bench-a": {
            "retention_groups": 16, "max_reorder_span": 32,
            "max_groups": 8, "group_ttl": 2.5, "max_count": 64}},
    }
    store = FileStore(STATE_DIR)
    service = CollectorService(
        config, store, FileSink(store), transport=transport,
        logger=lambda event, detail: print("    [服务] %s: %s" % (event, detail)))
    service.restore()
    thread = threading.Thread(target=service.run,
                              kwargs={"transport": transport, "idle": 0.02},
                              daemon=True)
    thread.start()
    return service, transport, thread


def wait_until(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


def fixture(sequence, parts, boot="boot-7"):
    return {"device": "bench-a", "boot": boot, "sequence": sequence,
            "parts": parts, "retention_groups": 16, "max_reorder_span": 32}


def main():
    print("状态目录: %s" % STATE_DIR)
    service, transport, thread = start_service()
    data_addr = transport.bound_data_addr

    with SyntheticDevice(("127.0.0.1", 0),
                         lambda text: print("    [设备] " + text)) as device:
        # 场景 1：序号回绕（从 65535 附近开始，展示 16 位回绕的连续解释）
        log("场景 1 序号回绕 65534 -> 65535 -> 0 -> 1（同一启动代次）")
        for sequence in (65534, 65535, 0, 1):
            device.send_measurement(
                data_addr, fixture(sequence, ["样本,%d\n" % sequence]))
            time.sleep(0.05)
        wait_until(lambda: service.assembler.counters["published"] >= 4)
        time.sleep(0.2)
        session = service.assembler.active_session["bench-a"]
        print("    会话高水位序号: %s，状态: %s（回绕未触发失败）"
              % (session.high_sequence, session.state))

        # 场景 2：乱序
        log("场景 2 乱序到达（倒序发送三片）")
        f2 = fixture(2, ["温度,", "18.2\n", "压力,2.3\n"])
        device.send_measurement(data_addr, f2, shuffle=True)
        key2 = ("bench-a", "boot-7", 2)
        wait_until(lambda: service.assembler.groups.get(key2) is not None
                   and service.assembler.groups[key2].status == PUBLISHED)

        # 场景 3：丢片 + 有上界重取
        log("场景 3 丢一片，采集端经有上界重取补回")
        f3 = fixture(3, ["振动,", "0.04\n", "转速,3100\n"])
        service.retransmit_sources["bench-a"] = MemoryRetransmit(
            Retention(f3), bind_address=device.address)
        device.send_measurement(data_addr, f3, drop=(1,))
        key3 = ("bench-a", "boot-7", 3)
        wait_until(lambda: service.assembler.groups.get(key3) is not None
                   and service.assembler.groups[key3].status == PUBLISHED)
        track = service._retrievers["bench-a"].status(key3)
        print("    重取尝试次数: %d（上限 %d）"
              % (track["attempts"], service.config["refetch_max_attempts"]))

        # 场景 4：设备中途退出，TTL 淘汰
        log("场景 4 设备中途退出：缺片组到 TTL 被放弃，不永久占缓存")
        f4 = fixture(4, ["流量,", "12.7\n", "湿度,41%\n"])
        device.send_measurement(data_addr, f4, drop=(0, 2))
        key4 = ("bench-a", "boot-7", 4)
        if not wait_until(lambda: key4 in service.assembler.groups):
            raise RuntimeError("组 4 未到达: %s"
                               % list(service.assembler.groups))
        print("    退出前组状态: %s，缺片 %s"
              % (service.assembler.groups[key4].status,
                 service.assembler.groups[key4].missing_indexes))

        # 场景 5：同片冲突隔离
        log("场景 5 同片内容冲突：整组隔离且不会发布")
        f5 = fixture(5, ["电压,", "220.1\n", "电流,1.1\n"])
        device.send_measurement(data_addr, f5, drop=(2,))
        evil_frame = json.loads(packets(f5)[0])
        evil = b"CORRUPT"
        evil_frame["payload"] = base64.b64encode(evil).decode()
        evil_frame["part_sha256"] = hashlib.sha256(evil).hexdigest()
        device.send_raw(json.dumps(evil_frame).encode(), data_addr)
        key5 = ("bench-a", "boot-7", 5)
        wait_until(lambda: service.assembler.groups.get(key5) is not None
                   and service.assembler.groups[key5].status == ABANDONED)
        print("    冲突组状态: %s，原因: %s"
              % (service.assembler.groups[key5].status,
                 service.assembler.groups[key5].abandon_reason))

        # 场景 6：序号跳跃超出乱序跨度，连续位置无法判断
        log("场景 6 序号从 5 跳到 38（超出跨度 32）：代次失败并请求新 boot")
        device.send_measurement(data_addr, fixture(38, ["越界,", "测量\n"]))
        wait_until(lambda: service.assembler.active_session["bench-a"].state
                   == "failed")
        print("    旧代次状态: %s（连续性证据丢失，停止接收）"
              % service.assembler.active_session["bench-a"].state)
        restarted = fixture(0, ["重启后,", "首组\n"], boot="boot-8")
        device.send_measurement(data_addr, restarted)
        key6 = ("bench-a", "boot-8", 0)
        wait_until(lambda: service.assembler.groups.get(key6) is not None
                   and service.assembler.groups[key6].status == PUBLISHED)
        print("    新代次 boot-8 已建立并发布首组")

        print("\n    设备现在退出（socket 关闭），等待 TTL ...")
    # with 退出即设备关闭；离线设备的缺片组由 TTL 淘汰。
    wait_until(lambda: service.assembler.groups[key4].status == ABANDONED,
               timeout=6.0)
    print("    退出组最终状态: %s，原因: %s"
          % (service.assembler.groups[key4].status,
             service.assembler.groups[key4].abandon_reason))

    # 状态查询（在线快照 + 离线磁盘）
    log("状态查询")
    snapshot = service.snapshot()
    print("    在线计数: %s" % json.dumps(snapshot["counters"],
                                          ensure_ascii=False))
    print("    协议错误分类: %s" % snapshot["protocol_errors"])
    service.stop()
    thread.join(timeout=3.0)

    offline = offline_status(STATE_DIR)
    print("    离线磁盘汇总: %s" % json.dumps(offline["totals"],
                                              ensure_ascii=False))
    print("    发布记录（交给计算程序的完整负载）:")
    for record_path in offline["records"]:
        record = json.loads(Path(record_path).read_text(encoding="utf-8"))
        print("      boot=%s seq=%s 长度=%d 摘要=%s..."
              % (record["boot"], record["sequence"], record["length"],
                 record["group_sha256"][:12]))
    print("\n演示结束。状态保留在 %s" % STATE_DIR)
    print("复查命令: python3 collector.py --offline --state-dir %s" % STATE_DIR)


if __name__ == "__main__":
    main()

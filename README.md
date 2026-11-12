# 多片遥测报文的乱序采集与缺失重取

把一次测量的多个 UDP 分片按 `(设备, 启动代次, 16 位序号)` 归组，处理乱序、
丢包重取、序号回绕与设备重启，只在全部片齐全且整组摘要一致时发布完整记录，
绝不把不完整数组交给下游。样例中的机构、设备、人员和标识均为虚构，
`source.py`/`device.py` 是本机合成工具，不连接真实仪器。

## 分层边界（各自可独立测试）

| 模块 | 边界职责 | 是否做 I/O |
| --- | --- | --- |
| `protocol.py` | 数据报 JSON/Base64/摘要/取值范围校验，超大组拒绝 | 否 |
| `assembler.py` | 归组、连续性判断、冲突隔离、容量与 TTL 上界（注入时钟） | 否 |
| `retrieval.py` | 有上界的缺片重取（次数/时长/暂存失效） | 仅边界可注入 |
| `persistence.py` | 三态落盘、原子写、进程恢复 | 文件 |
| `publisher.py` | 幂等发布完整记录给下游 | 文件/内存 |
| `transport.py` | 真实回环 UDP 收发与控制口 | socket |
| `service.py` | 把上述边界编排成单数据报事务与周期任务 | 编排 |
| `collector.py` | 服务命令行与在线/离线状态查询 | 入口 |
| `device.py` | 合成设备：乱序/丢片/重取应答/回绕/重启 | socket |
| `compute.py` | 下游计算示例：只消费已发布记录并复核摘要 | 只读 |

原有 `reader.py`/`app.py` 查看器保持不变，仍可独立运行。

## 运行

使用 Python 3.11 标准库，无第三方依赖。

```bash
# 现有查看器（不变）
python3 app.py

# 单元 + 真实回环 UDP 端到端测试
python3 -m unittest discover -s tests -v

# 全链路演示：回绕、乱序、丢片重取、中途退出、冲突隔离、代次失败
python3 demo.py

# 编译检查
PYTHONPYCACHEPREFIX=/tmp/collector-pycache python3 -m compileall -q \
  app.py reader.py source.py protocol.py assembler.py persistence.py \
  publisher.py retrieval.py transport.py service.py collector.py device.py \
  compute.py demo.py tests
```

## 采集服务（真实 UDP）

```bash
# 终端 1：启动采集服务（数据口 51001，控制口 51002，状态落 state/）
python3 collector.py --config config/collector.json --state-dir state

# 终端 2：合成设备发乱序报文并应答缺片重取
python3 device.py --fixture fixtures/scenarios.json \
  --target 127.0.0.1:51001 --shuffle --drop 1 --serve 5

# 在线状态查询（走控制口）；服务未运行时回退磁盘状态
python3 collector.py --status
python3 collector.py --offline            # 只读磁盘

# 下游只消费完整记录
python3 compute.py --state-dir state
```

## 关键语义

- **归组不靠猜**：序号按启动代次解释回绕；跨越半个序号空间或超出
  32 组乱序跨度（向前或向后）即判定连续位置不可知，整代次失败、请求新 boot。
- **隔离而非将就**：同片内容冲突、总片数改变、摘要不符的组永不发布。
- **有上界**：每设备缓存组数、每组 TTL、重取次数与总时长都有上限，
  离线设备不会永久占用缓存。
- **恢复可区分三态**：未组齐可继续、已发布不重复、已放弃不可翻案。
- **发布唯一**：只有整组摘要一致才发布，重复收齐只交付一次。

## 容器

基础镜像固定摘要，不在启动阶段安装软件或获取远程数据。

```bash
docker build --pull=false -t datagram-capture:local .
docker run --rm --network none datagram-capture:local
docker run --rm --network none datagram-capture:local \
  python3 -m unittest discover -s tests -v
```

持久化数据放在 `state/`（已在 `.gitignore`）或调用方显式传入的目录，
不写入样例文件。

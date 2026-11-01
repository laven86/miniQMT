# 大QMT文件桥通道方案 —— 沙箱封禁 TCP 后的 RPC 通道

> 2026-10-31 定稿。文件桥 RPC 通道已实盘验证通过（DRYRUN 10 分钟无弹窗、web 前端可读持仓）。
> 部署操作手册见 `大QMT-RPC联调checklist.md`（零、文件桥模式一节）；本文讲清架构、协议与排障原理。

## 一、背景与根因

2026-10-23 券商升级后，大QMT 主客户端（XtClient）**周期性枚举内嵌策略进程的 TCP 连接表**
（`TC::CStrategyTradeManager`），只要发现任何对端不在券商白名单（柜台/行情服务器）——
**包括 127.0.0.1 回环、任何端口**——立即弹「不可用IP」并强制停止所有策略：

```
TC::CStrategyTradeManager::onIllegalIPFind illegal IP: 127.0.0.1:51761
（随后 stop 所有策略）
```

由此**证伪**的方案（换端口、换 IP、zmq、mysql 皆同罪——任何 socket 出连都拦）：

| 方案 | 结论 |
|------|------|
| 直连 Redis 127.0.0.1:6379 | 拦（onIllegalIPFind） |
| 本机 TCP 中继（监听 51761，策略连回环） | 拦（回环也被枚举） |
| zmq / mysql 传输 | 同为 TCP 出连，拦 |

与此同时，策略进程内的**文件 IO 大部分存活**（2026-10-31 v3 探针 `BIGQMT_FILE_PROBE.py` 实测）：

- **写真实落盘**：write=OK(verified)（os.stat 佐证非假写）；os.replace / os.remove / os.stat / os.listdir 均 OK
- **读被拦**：builtins.open、io.open、os.open(O_RDONLY) 全抛 `PermissionError: Forbidden FileIO`
- **绕行通道**：底层 `_io.FileIO(path, 'rb')` 与 importlib 动态导入**可用**——沙箱只 hook 了
  Python 层的 open/os.open 读路径，没有拦到 C 层 `_io.FileIO`

结论：沙箱是「写放行、Python 层读拦截、_io.FileIO 漏网」的模型，**唯一可行通道是文件交换**
（读走 `_io.FileIO` 绕行）。

## 二、架构

```
┌──────────────────────┐                ┌─────────────────────────────┐
│  大QMT 策略进程（沙箱） │                │  miniQMT 主机（普通 Python）   │
│  bigqmt_signal_trader │                │                             │
│                      │   文件交换       │  FileRelayDaemon（文件桥）    │
│  FileRedisClient     │◄═══════════════►│  qmt_file_relay.py          │
│  （纯标准库，零 socket）│  req/<id>.json  │  · 嵌入 main.py 守护线程      │
│  写 req / 读 resp     │  resp/<id>.json │    （ENABLE_QMT_FILE_BRIDGE）│
│  （读走 _io.FileIO）   │  （JSON 帧）    │    或独立 CLI 运行（排障）     │
└──────────────────────┘                │        │ redis-py           │
                                        └────────┼───────────────────┘
                                                 ▼
                                        ┌────────────────┐
                                        │  Redis（db5）   │
                                        └────────▲───────┘
                                                 │ 直连（对桥透明）
                              ┌──────────────────┼──────────────────┐
                              │                  │                  │
                        QmtRpcTrader       web_server         position_manager
                        （下单/查询）      （前端读持仓）      （持仓监控）
```

职责切分：

- **大QMT 策略端**（`adapters/file_redis_client.py`，纯标准库、零 socket）：把每条 Redis
  命令写成 `req/<id>.json`，轮询读回 `resp/<id>.json`（读走 `_io.FileIO`，绕沙箱读拦截）。
- **文件桥**（`qmt_file_relay.py`，普通 Python 不受沙箱限制）：轮询 `req/`，经
  `qmt_relay_common` 白名单校验后代为执行 Redis 命令，把结果原子写回 `resp/`。可嵌入
  `main.py` 作为守护线程（`start_file_bridge_thread`，init_system 之后、业务线程之前启动），
  也可独立 CLI 运行。
- **miniQMT 侧**（QmtRpcTrader / web_server / position_manager）：直连 Redis，对桥完全透明。

## 三、协议细节

目录布局（默认 `qmt-trader\qmt_file_bridge`，桥 `--dir` / 环境变量 `QMT_FILE_DIR` 可覆盖；
**策略侧 local_config 的 `file_dir` 必须与之一致**）：

```
<root>/req/     请求文件 <id>.json   （客户端原子写入，桥处理完删除）
<root>/resp/    响应文件 <id>.json   （桥原子写入，客户端取走删除）
<root>/qmt_file_relay.lock           （单实例锁，内容 pid=<n> started=<时间>）
```

**帧格式**（JSON，ensure_ascii 纯 ASCII 落盘）：

```json
请求 {"id": <int递增>, "nonce": "<每进程唯一>", "token": "<str>", "cmd": "<str>", "args": [...]}
响应 {"id": <int>, "nonce": "<原样回显>", "ok": true, "data": ...}
     {"id": <int>, "nonce": "<原样回显>", "ok": false, "error": "<msg>"}
```

- **值编码**：bytes 用 `{"__b64__": base64}` 包裹（redis-py 类型语义保真，含非 ASCII）；
  dict 用 `{"__map__": [[k, v], ...]}` 键值对列表（JSON 对象键只能是字符串，hgetall 的
  bytes 键靠它往返）。编码/解码逻辑集中在 `qmt_relay_common.py` 的
  `encode_value`/`decode_value`，桥与策略端共用同一套语义。
- **原子写**：先写 `<name>.tmp` 再 `os.replace`（Windows 同卷原子）。读取方只认最终名，
  永远读不到半个文件。
- **nonce 回显防错认**：客户端每进程生成唯一 nonce，响应原样回显；客户端校验不一致或缺失
  一律忽略并删除该响应文件——防止客户端重启后（id 从 1 重新计数）错认旧响应。
- **超时不重发**：默认 1.5s（local_config `file_timeout` 可配）超时即放弃该次调用，绝不重发
  ——防 LPOP/RPUSH 之类的非幂等命令被重放（请求可能已被消费只是响应迟到）。
- **id 必须是 int**（bool 除外）：响应文件名由 id 拼接，非 int 一律写错误响应拒绝，堵掉
  `"id": "../../evil"` 之类的路径注入。
- **单实例锁（含 pid 自愈）**：root 下以 `O_CREAT|O_EXCL` 创建 `qmt_file_relay.lock`，
  内容 `pid=<n> started=<时间>`。抢锁失败时读锁内 pid，用 Windows API
  （ctypes `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)` + `GetExitCodeProcess`，
  **不能用 `os.kill(pid, 0)`**——Windows 上它会真杀进程）判存活：持锁进程已死则删残留锁
  重试一次（覆盖桥嵌入 main.py 时 daemon 线程随进程硬退不清锁的场景）；探测异常按「活」
  处理（保守，宁可不自愈也不误删活锁）；仍失败才抛 `FileRelayLockedError`。
- **命令白名单与 key 前缀**：只放行策略实际用到的命令
  （get/set/setnx_ex/setex/delete/expire/rpush/lpop/publish/hset/hgetall/xadd/ping），
  key 必须以 `bigqmt:` 开头；共享 token 鉴权（空则双向不校验）。校验、执行分发与编解码
  集中在公共模块 `qmt_relay_common.py`，桥 import 复用，无第二份实现。
- **_io.FileIO 绕沙箱读**：策略端读 resp 走 `_io.FileIO(path, 'rb')`（沙箱实测未拦），
  `_io` 不可用时回退 builtins.open（本机/测试行为不变）。
- **启动清理**：桥 `prepare()` 清空 req/ resp/ 全部残留（含 .tmp 与隔离文件）——正因如此，
  嵌入 main.py 时桥线程必须先于业务线程启动；删不掉的坏请求拉入隔离名单绝不重扫（防重放）。

## 四、部署与使用

### 启动链（miniQMT 侧，二选一）

1. **集成模式（推荐）**：直接 `python main.py`。桥自动嵌入为守护线程：
   - 开关：`ENABLE_QMT_FILE_BRIDGE`（默认 **True**——文件桥已是唯一可用通道）
   - 参数（config.py 文件桥段，环境变量同名）：
     - `QMT_FILE_DIR`：默认 `qmt-trader\qmt_file_bridge`（与独立模式默认一致）
     - `QMT_FILE_TOKEN`：默认空（不校验；**须与大QMT侧 local_config 的 `file_token` 一致**）
     - `QMT_FILE_KEY_PREFIX`：默认 `bigqmt:`
     - `QMT_FILE_POLL_INTERVAL`：默认 0.002s
     - Redis 连接复用 `QMT_RPC_REDIS`（host/port/db/password）
   - 失败降级：Redis ping 重试 5 次失败、锁冲突自愈后仍失败——均只告警不阻断主流程
   - 退出：注册在 `threads`（`("file_bridge", stop+join(2))`），由 `cleanup()` 统一收尾
   - 好处：无需单独窗口；桥可继承 `config._load_dotenv_fallback` 注入的环境变量
     （独立启动吃不到 .env）
2. **独立模式（排障用）**：`python qmt-trader\qmt_file_relay.py --token 你的令牌`
   （`--dir/--host/--port/--db/--password` 或 `QMT_FILE_*` / `QMT_RPC_REDIS_*` 环境变量）
   启动即 ping 重试连 Redis 并清空残留文件；单实例锁冲突退出码 2。

### 大QMT 侧配置

- `bigqmt_signal_trader_local_config.py` 的 `BIGQMT_REDIS_CONFIG` 配 `file_dir`（与桥
  `--dir`/`QMT_FILE_DIR` 一致）与 `file_token`（与桥 token 一致，空则双向不校验）。
- 残留的 `relay_host`/`relay_port`/`relay_token` 三项**已被忽略**（TCP 中继代码已删），建议删除避免误导。
- ⚠️ `adapters/`（含 file_redis_client.py）**不在策略壳 reload 清单内**：改动后必须完整
  退出并重启大QMT客户端再跑策略。
- 每个策略进程独占一个 file_dir：双账号须各配独立目录、各跑一个桥实例，否则请求/响应互相错认。

### 探针与验收

`BIGQMT_FILE_PROBE.py`（v3，三阶段）拷到大QMT `python` 目录、策略编辑器单独加载运行：

1. **phase1** 五目录文件 IO 能力矩阵（write 用 os.stat 佐证 `OK(verified)`/`FAKE`）
2. **phase2** 八通道绕行读取矩阵（含 `_io.FileIO`、importlib 动态导入）
3. **phase3** 文件桥 ping 往返，**rtt < 10ms 为合格**（多为 Windows 时钟粒度抖动）；
   `daemon rejected: bad token` 即 token 不一致

完整验收与联调步骤见 `大QMT-RPC联调checklist.md`。

## 五、与业务接口的关系

**RPC 协议零改动**：买卖/持仓/委托等业务 RPC 仍走原有 `bigqmt:*` Redis 键与
`RedisPubSubRpcService`（bigqmt:rpc:req:{account} 队列、bigqmt:positions:{account_id}
持仓哈希、bigqmt:order_events 成交推送等）。文件桥只是把「策略进程 ↔ Redis」这一段
传输从 socket 换成了文件：

- miniQMT 侧 QmtRpcTrader / web_server / position_manager 直连 Redis，对桥**完全透明**
- web 层**零适配**（前端读持仓、模拟买入等接口不变）
- 策略端 `FileRedisClient` 与原 redis-py 客户端 duck-typed 同接口，上层 redis_rpc 无感
- 文件桥模式下 `xadd` 不做 maxlen 裁剪，成交/委托事件只走 pub/sub 推送，不写 stream

## 六、排障表

| 现象 | 根因 | 处置 |
|------|------|------|
| 探针 phase3 / RPC 无响应 | 桥没跑（集成模式开关关了或降级告警被忽略）；`file_dir` 与桥 `--dir`/`QMT_FILE_DIR` 不一致 | 看 miniQMT 日志有无 `✅ 文件桥已启动`；比对两侧目录 |
| `daemon rejected: bad token` | 桥 token 与 local_config `file_token` 不一致 | 两边改成一致（空=双向不校验） |
| 非法IP弹窗（onIllegalIPFind） | 旧 relay 配置残留或任何 TCP 外连代码 | relay 分支已删、relay_* 键被忽略；排查策略目录里是否有旧版脚本/自加 socket 代码 |
| 探针 phase1 readback 全 FAIL / phase2 `_io.FileIO` 也 FAIL | 券商升级沙箱，把 `_io.FileIO` 也拦了 | 跑探针 phase2 回归确认；文件桥失效，走券商白名单申请 |
| 桥启动报 lock / exit code 2 | 锁文件残留且 pid 探测为活（pid 被复用）或另一实例真在跑 | 确认无实例后删 `<root>\qmt_file_relay.lock` 再启动（正常场景 pid 已死会自愈，无需人工） |
| 双账号请求互相错认 | 两个策略共用一个 file_dir | 每账号独立 file_dir + 独立桥实例 |
| req/ 目录大量 json 堆积（连续编号） | 桥离线/未运行期间策略仍在发请求；旧版客户端超时后不删自己的 req 文件，每次调用留一个 | 新版客户端已超时自删（best effort）；存量文件手工清空即可，桥下次启动 prepare() 也会自动清空 req/ |
| 排查用的读写权限 | QMT 进程对 file_dir 无读写权限 | phase1 矩阵确认 file_dir 可写可读（读走 phase2 的 `_io_fileio` 通道） |

## 七、历史方案注记

- **TCP 中继（已证伪，代码已删）**：2026-10-23 前可用——本机中继监听 127.0.0.1:51761，
  策略用 stdlib socket 连它。升级后回环也被 `onIllegalIPFind` 枚举拦截，方案证伪。
  2026-10-31 删除 `qmt_redis_relay.py` / `adapters/relay_redis_client.py` /
  `BIGQMT_RELAY_PROBE.py`；其白名单、token、key 前缀与编解码逻辑抽到
  `qmt_relay_common.py` 供文件桥复用，策略侧 relay 分支与样例同步移除。**勿再尝试任何
  TCP/UDP/socket 通道。**
- **旧 IPC 文件方案（保留为历史文档）**：`大QMT文件IPC方案.md`（C:\QuantIPC 目录、
  pending/processing/done 订单文件流）是更早的独立方案，从未实盘启用，已被本文件桥方案
  取代；文档保留仅作历史参考，其 `ENABLE_QMT_IPC_FALLBACK` 开关保持默认 False。

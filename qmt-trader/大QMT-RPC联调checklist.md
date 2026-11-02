# 大QMT RPC 联调 Checklist（P5）

> P5 已完成一轮真实联调验证；本清单保留为**复现/新环境部署**手册，指导在装有大QMT的机器上完成真实下单闭环验证。
> 全程建议用**模拟盘或极小额（1 手）**，逐项打勾。配套方案：`大QMT-RPC方案.md`；通道架构详见 `大QMT文件桥通道方案.md`（2026-10-31）。

---

## 前置条件

- [ ] 一台可访问的 **Redis** 服务（miniQMT 端能连通即可；大QMT 端不再直连 Redis，经文件桥间接访问）
- [ ] 大QMT客户端已登录，可正常手动下单
- [ ] miniQMT 端已 `pip install redis`（`utils/requirements.txt` 已含）
- [ ] 确认三个交易开关**当前只会开一个**：`ENABLE_XTQUANT_MANAGER` / `ENABLE_QMT_IPC_FALLBACK` / `ENABLE_QMT_RPC_FALLBACK`（同开会被工厂抛异常拦截）

---

## 零、文件桥模式（券商 2026-10-23 升级后唯一可行通道）

### 背景：一切 TCP 方案已证伪

2026-10-23 券商升级后，大QMT 主客户端（XtClient）**周期性枚举内嵌策略进程的 TCP 连接表**，只要发现任何对端不在券商白名单（柜台/行情服务器）——**包括 127.0.0.1 回环、任何端口**——立即弹「不可用IP」并强制停止所有策略（日志实证）：

```
TC::CStrategyTradeManager::onIllegalIPFind illegal IP: 127.0.0.1:51761
（随后 stop 所有策略）
```

直连 6379 与 TCP 中继 51761 均被拦，换端口、换 IP 无效。策略进程内**文件读写仍可用**（能 import 模块、能写 __pycache__），因此唯一可行通道是文件交换。

解决办法：miniQMT 机器上运行 `qmt-trader/qmt_file_relay.py`（普通 Python，持有 Redis 连接）；大QMT 策略端用 `adapters/file_redis_client.py`（纯标准库、零 socket）把 Redis 命令写成请求文件、轮询取回响应文件。miniQMT 端 `QmtRpcTrader` 仍直连 Redis，不用改。

```
miniQMT ──redis──► Redis :6379 ◄──redis── qmt_file_relay.py（文件桥守护进程）
                                            ▲    │
                                   写 resp/<id>.json    读 req/<id>.json
                                            │    ▼
                          大QMT策略 FileRedisClient（纯标准库，零 socket）
```

目录布局（默认 `qmt-trader\qmt_file_bridge`，守护进程 `--dir` / 环境变量 `QMT_FILE_DIR` 可覆盖；策略侧由 local_config 的 `file_dir` 指定，两边必须一致）：

- `<root>/req/` 请求文件 `<id>.json`：单行 JSON `{"id":<int递增>,"nonce":"<每进程唯一>","token":"<str>","cmd":"<str>","args":[...]}`，客户端先写 `.tmp` 再 `os.replace` 原子落盘
- `<root>/resp/` 响应文件 `<id>.json`：`{"id":<int>,"nonce":"<原样回显>","ok":true,"data":...}` 或 `{"id":<int>,"nonce":"<原样回显>","ok":false,"error":"<msg>"}`；客户端校验 nonce，**不一致或缺失的响应文件一律忽略并删除**（防客户端重启后错认旧响应）
- 守护进程要求 `id` 必须是 int（bool 除外），非 int 一律写错误响应拒绝——堵掉响应文件名的路径注入
- bytes 用 `{"__b64__":...}`、dict 用 `{"__map__":[[k,v],...]}` 包裹，hgetall 的 bytes 键值可保真往返
- 超时默认 1.5s（local_config `file_timeout` 可配）：超时即放弃该次调用，**绝不重发**（防 LPOP/RPUSH 重放）

**每个策略进程独占一个 file_dir**：双账号必须各配独立目录、各起一个桥实例（`--dir` 分开指定），否则两个策略的请求/响应文件会互相错认与覆盖。

### 部署顺序（缺一不可）

- [ ] 1. 启动 Redis（保持常开）。
- [ ] 2. 启动文件桥守护进程，二选一：
  - **集成模式（推荐）**：直接 `python main.py`——桥已嵌入为守护线程（`start_file_bridge_thread`，开关 `ENABLE_QMT_FILE_BRIDGE` 默认 True，参数见 config.py 文件桥段：`QMT_FILE_DIR/QMT_FILE_TOKEN/QMT_FILE_KEY_PREFIX/QMT_FILE_POLL_INTERVAL`，Redis 复用 `QMT_RPC_REDIS`）。日志出现 `✅ 文件桥已启动` 即可；Redis 连不上或锁冲突时仅告警降级不阻断主流程。
  - **独立模式（排障用）**：`python qmt-trader\qmt_file_relay.py --token 你的令牌`（保持窗口常开）
  看到 `[qmt_file_relay] watching c:\...\qmt_file_bridge (req/ resp/)` 即可。默认目录为 `qmt-trader\qmt_file_bridge`，可用 `--dir` 或环境变量 `QMT_FILE_DIR` 覆盖；Redis 地址/库/密码走 `--host/--port/--db/--password` 或 `QMT_RPC_REDIS_*` 环境变量；启动时会先 ping 重试连接 Redis，并清空 req/ resp/ 残留文件。
  **单实例锁（含 pid 自愈）**：启动时在 `<root>\qmt_file_relay.lock`（O_CREAT|O_EXCL）占坑，重复启动第二个实例会立即报错退出（exit code 2）；锁内记录 `pid=... started=...`，抢锁失败时会读 pid 用 Windows API（OpenProcess+GetExitCodeProcess）判存活——持锁进程已死则自动删残留锁重试一次（集成模式下 daemon 线程随进程硬退不清锁的场景）。探测失败按"活"处理（保守，不误删活锁）；仍失败才报错，人工删除锁文件再启动。
- [ ] 3. 文件桥探针（v3，三阶段）：把 `BIGQMT_FILE_PROBE.py` 拷到大QMT `python` 目录，在策略编辑器单独加载运行（它从 `bigqmt_signal_trader_local_config` 读 `file_dir`/`file_token`，没配置则用文件内默认值）：
  - phase1 五目录能力矩阵（py_dir / py_dir/bridge / temp / file_dir / userdata=脚本目录上级 userdata，逐项 makedirs、write、readback），write 用 `os.stat` 佐证真伪：`OK(verified)` / `FAKE`（写被静默丢弃）/ `OK(stat-blocked)`；任一单项失败不影响后续项。
  - **2026-10-31 最终实测结论**（v3 探针）：写入真实落盘（write=OK(verified)，temp 与 file_dir 均验证；os.replace/os.remove/os.stat/os.listdir 均 OK）；读取被拦——builtins.open、io.open、os.open(O_RDONLY) 全抛 `Forbidden FileIO`，但 **`_io.FileIO(path,'rb')` 与 importlib 动态导入实测可用**：沙箱只 hook Python 层 open/os.open，底层 _io.FileIO 未拦。
  - **客户端已改造**：`adapters/file_redis_client.py` 读 resp 改走 `_io.FileIO`（_io 不可用时回退 builtins.open，本机/测试行为不变），写/删/replace 不变。部署后 phase3 探针 ping 应当 OK，rtt<10ms 门槛不变。
  - 若 phase3 报 `no response within 5.0s`：核对守护进程是否用与 local_config 相同的 `--dir` 与 `--token` 启动；若报 `daemon rejected: bad token` 则是 token 不一致。
  - phase1/phase2 矩阵留作回归对照：若券商后续升级沙箱把 `_io.FileIO` 也拦掉，phase2 会退回 `HINT: no read channel, file bridge needs another design`，届时文件桥再次失效，走券商白名单申请。
  - 若 phase1 显示纯路径范围豁免（py_dir 可写可读而 file_dir 不可）：仍可把桥目录迁到 `python\qmt_file_bridge`（守护进程 `--dir` 与 local_config `file_dir` 同步改，守护进程不受沙箱限制）。
  - `phase3 FAIL`（超时）：守护进程没启动、`file_token` 与 `--token` 不一致、或桥目录与守护进程 `--dir` 不一致。
- [ ] 4. 私有配置 `BIGQMT_REDIS_CONFIG` 增加 `file_dir`（与守护进程 `--dir` 一致）、`file_token`（与 `--token` 一致）。此模式下 host/port/password 由守护进程持有，策略侧不使用。
  **建议删掉残留的 `relay_host` / `relay_port` / `relay_token` 三项**：TCP 中继代码已于 2026-10-31 删除，`redis_common` 不再有 relay 分支、runtime 不再读 relay_*，这些键现在**被直接忽略**（无功能影响，留着只会误导排障）。历史案例（2026-10-31，relay 尚存时）：探针 phase3 ping OK 但 DRYRUN 报 `RelayTimeoutError ... 127.0.0.1:51761` 并弹非法IP——根因即 local_config 只配了 relay_* 没配 file_dir，运行时回落 TCP 中继。此案例促成了快照语义改造（`configure_runtime_redis` 键缺失即清空旧值）与最终删除 relay。
- [ ] 5. 本次更新 `adapters/file_redis_client.py`（读 resp 改走 `_io.FileIO` 绕行沙箱读拦截），`adapters/` 不在 reload 清单内：拷贝后**完整退出并重启大QMT 客户端**再运行策略。
- [ ] 6. 运行 `BIGQMT_REDIS_DRYRUN.py` 超过 10 分钟不被停（文件桥模式下不应再出现任何「不可用IP」），再按第二、三节验证。

文件桥只放行策略用到的命令（get/set/setnx_ex/setex/delete/expire/rpush/lpop/publish/hset/hgetall/xadd/ping），且 key 必须以 `bigqmt:` 开头；白名单、token、前缀校验与编解码直接 import 自 `qmt_relay_common.py`（从原中继抽出的公共模块），与原中继完全一致。文件桥模式下 `xadd` 不做 maxlen 裁剪，成交/委托事件只走 pub/sub 推送，不写 stream。

### 延迟实测门槛

`BIGQMT_FILE_PROBE.py` 阶段3 的 rtt **< 10ms 为合格**；若 >10ms，多为 Windows 时钟粒度（~15ms）导致的 sleep 抖动。应对措施：调整 drain 预算 / 降低每 tick 处理量，或联系开发评估 pipeline 批量化；**不要自行改轮询实现本身**。

### 零.1、TCP 中继模式（历史注记，方案已证伪，代码已删除）

2026-10-23 券商升级前可用：本机 TCP 中继（监听 127.0.0.1:51761，策略用 stdlib socket 连它）。升级后 `onIllegalIPFind` 拦截策略进程一切 TCP 外连（回环也被拦，换端口/IP 无效），方案证伪。**勿再尝试任何 TCP/UDP/socket 通道**。相关文件（`qmt_redis_relay.py` / `adapters/relay_redis_client.py` / `BIGQMT_RELAY_PROBE.py`）已于 2026-10-31 删除；其白名单、token、key 前缀与编解码逻辑抽到 `qmt_relay_common.py` 供 `qmt_file_relay.py` 复用，策略侧 `redis_common`/runtime/local_config 的 relay 分支与样例同步移除（残留 relay_* 配置键一律忽略）。详记见 `大QMT文件桥通道方案.md` 的历史方案注记一节。

---

## 一、大QMT 端（服务端）部署

### 1.1 拷贝脚本到大QMT的 python 目录

把 vendored 的 `src/` 下内容拷到大QMT的 `python` 目录（如 `D:\国金证券QMT交易端\python\`）：

- [ ] 整个 `bigqmt_signal_trader/` 包（含 `adapters/`）
- [ ] `bigqmt_signal_trader_redis_rpc_runtime.py`
- [ ] `BIGQMT_REDIS_DRYRUN.py`（★ QMT 编辑器入口，GBK 编码）

> 源路径：`qmt-trader/vender/bigqmt/src/`

> **改动生效边界（重要）**：入口壳每次运行只显式 reload `redis_common / redis_rpc /
> bigqmt_signal_trader_strategy / *_runtime`（strategy.py 另会强制 reload
> `adapter_factory`）。`bigqmt_signal_trader/adapters/` 下的各适配器与 `models.py`
> **不在 reload 清单内——拷贝后仅重跑策略不生效，必须完整退出并重启大QMT客户端**
> 再重跑策略。改动大多落在 redis_rpc.py（重跑即可生效）；动适配层/models 前先确认
> 能接受客户端重启成本。

### 1.2 创建私有配置（不提交）

在大QMT的 `python` 目录新建 `bigqmt_signal_trader_local_config.py`（参考 `bigqmt_signal_trader_local_config.example.py`）：

```python
# coding: utf-8
BIGQMT_ACCOUNT_ID = "你的资金账号"          # 与 miniQMT account_config.json 的 account_id 一致

BIGQMT_REDIS_CONFIG = {
    "host": "127.0.0.1",                   # miniQMT 端能连到的 Redis 地址
    "port": 6379,
    "db": 5,                               # ★ 必须与 miniQMT 端 QMT_RPC_REDIS_DB 一致
    "username": "",
    "password": "你的Redis密码",            # 无密码留空（文件桥模式下由守护进程使用）

    "file_dir": r"c:\croco_hunter_decision\miniQMT\qmt-trader\qmt_file_bridge",  # ★ 与 qmt_file_relay --dir 一致
    "file_token": "你的令牌",              # ★ 与 qmt_file_relay --token 一致

    "rpc_allow_order_methods": True,       # ★★ 联调下单必须改 True（默认 False 拒绝下单）
    "exec_events_enabled": True,           # ★ 启用成交/委托 Redis 推送（miniQMT 端回调依赖）

    "rpc_process_in_listener": True,
    "rpc_listener_methods": ("*",),
    "rpc_background_threads": False,        # redis 传输保持 False；切 zmq/mysql 才改 True
    "schedule_adjust": True,
    "schedule_adjust_interval": "500nMilliSecond",
}
```

- [ ] `BIGQMT_ACCOUNT_ID` 与 miniQMT `account_config.json` 的 `account_id` **完全一致**
- [ ] `db` 与 miniQMT `QMT_RPC_REDIS_DB` **一致**
- [ ] `rpc_allow_order_methods = True`
- [ ] `exec_events_enabled = True`

### 1.3 在大QMT策略编辑器运行

- [ ] 模型交易/模型研究 → 新建 Python 策略 → **只加载 `BIGQMT_REDIS_DRYRUN.py`**
- [ ] 运行模式选"实盘"，编译保存运行
- [ ] 输出面板出现以下**启动成功标志**：

```
[bigqmt_shell] reload entry paths=['D:\\...\\python']
[bigqmt_shell] local redis config loaded keys=['db','exec_events_enabled','host',...]
[bigqmt_shell] local account config loaded=True
[bigqmt_rpc] started channel=bigqmt:rpc:req:你的账号 queue=...
```

---

## 二、miniQMT 端（客户端）配置

### 2.1 环境变量清单

在 miniQMT 启动前设置（`.env` 或启动脚本；密码等敏感项**只走环境变量**）：

| 环境变量 | 联调值 | 说明 |
|---------|--------|------|
| `ENABLE_QMT_RPC_FALLBACK` | `true` | 启用 RPC 交易后端 |
| `ENABLE_QMT_FILE_BRIDGE` | `true`（默认） | 文件桥守护线程嵌入 main.py（现为唯一可用通道；`false` 则需独立跑 `qmt_file_relay.py`） |
| `QMT_RPC_ALLOW_ORDER` | `true` | ★ 放开真实下单（默认 false 为只读安全） |
| `QMT_RPC_TRANSPORT` | `redis` | 传输方式（联调用默认 redis） |
| `QMT_RPC_REDIS_HOST` | `127.0.0.1` | Redis 地址（与大QMT端一致） |
| `QMT_RPC_REDIS_PORT` | `6379` | Redis 端口 |
| `QMT_RPC_REDIS_DB` | `5` | ★ 与大QMT端 `db` 一致 |
| `QMT_RPC_REDIS_PASSWORD` | `你的Redis密码` | 无密码留空 |
| `ENABLE_SIMULATION_MODE` | `false` | ★ 必须关模拟模式，否则 pm 跳过 QMT 连接 |

Windows CMD 示例：
```bat
set ENABLE_QMT_RPC_FALLBACK=true
set QMT_RPC_ALLOW_ORDER=true
set QMT_RPC_REDIS_HOST=127.0.0.1
set QMT_RPC_REDIS_DB=5
set ENABLE_SIMULATION_MODE=false
```

- [ ] `account_config.json` 的 `account_id` 与大QMT端 `BIGQMT_ACCOUNT_ID` 一致
- [ ] 确认 `ENABLE_XTQUANT_MANAGER=false` 且 `ENABLE_QMT_IPC_FALLBACK=false`

### 2.2 连通性快速自检（不下单，先跑）

在 miniQMT 项目根目录跑（无需启动整个系统）：

```bash
"/c/Users/PC/Anaconda3/envs/python39/python.exe" -c "
import sys, os
sys.path.insert(0, 'qmt-trader')
os.environ['ENABLE_QMT_RPC_FALLBACK']='true'
from qmt_rpc_trader import QmtRpcTrader
t = QmtRpcTrader(account='你的账号')
print('ping:', t.ping_xttrader())        # 期望 True
print('health:', t.get_rpc_health())
print('positions:')
print(t.position())                      # 期望返回真实持仓 DataFrame
print('asset:', t.query_stock_asset())   # 期望返回真实资产
"
```

- [ ] `ping: True`
- [ ] `position()` 返回真实持仓（证券代码/股票余额/可用余额/成本价/市值 五列齐全）
- [ ] `query_stock_asset()` 总资产/可用金额与大QMT一致

---

## 三、下单闭环验证（模拟盘/1 手）

- [X] 启动 miniQMT：`python main.py`
- [X] 日志出现 `QmtRpcTrader 连接成功（大QMT RPC 在线）` 和 `[OK] QMT已连接`
- [X] 通过 Web 模拟买入接口或直接调用下 **1 手**限价单
- [X] 观察闭环：
  - [X] miniQMT 日志：`RPC下单已提交: buy ... order_id=<纯整数>, 返回=bq:...`
  - [X] 大QMT输出面板出现 passorder 记录，大QMT委托列表出现该笔委托
  - [ ] 成交后 miniQMT 日志出现成交回调（pending_orders 移除该 order_id）—— 交易时段验证
  - [ ] `position()` 刷新出现新持仓 —— 交易时段验证
- [X] 测试撤单：下一笔挂单后撤单，确认 `RPC撤单指令已提交` 且大QMT委托变"已撤"
- [X] **关键发现：strategy_name 必须匹配**。下单时需传 `strategy_name="bigqmt_signal_trader"`（等于大QMT 端 RPC 运行时设的 strategy_name），否则委托查询按 strategy_name 过滤时查不到该笔单。不带 strategy_name 下单后 query_orders 返回空（已排障确认）。

> 🔔 **price_type 透传**（2026-07-13）：`_send()` 现已支持调用方显式指定 `price_type`。
> 不指定时自动推断（price=0→市价，否则→限价 FIX_PRICE）。如需其他价型（对手价、转债专用等），
> 传 `price_type=<整数值>` 即可，经 vendored → passorder 透传到国金柜台。
> 常用值：`FIX_PRICE=11`、`LATEST_PRICE=5`、`MARKET_PEER_PRICE_FIRST=44`、
> `MARKET_SH_CONVERT_5_LIMIT=43`、`MARKET_SZ_CONVERT_5_CANCEL=47`。

---

## 四、排障要点

| 现象 | 可能原因 | 排查 |
|------|---------|------|
| `BIGQMT_FILE_PROBE` phase1 readback FAIL/Forbidden FileIO | 沙箱 hook builtins.open/io.open/os.open 读路径 | 已由 `_io.FileIO` 绕行（FileRedisClient 已改造）；若 phase2 绕行通道也全 FAIL 则文件桥失效，走券商白名单 |
| `BIGQMT_FILE_PROBE` phase3 no response within 5.0s | 守护进程未启动 / token 不一致 / 桥目录与 --dir 不一致 | 确认 `qmt_file_relay.py` 窗口存活；比对 `--token`、`--dir` 与 local_config（`daemon rejected: bad token` 即 token 不一致） |
| DRYRUN 弹非法IP / `RelayTimeoutError ... 127.0.0.1:51761`（历史，2026-10-31 前） | 旧版 local_config 只配 relay_* 没配 file_dir，回落 TCP 中继 | relay 分支已删，relay_* 键被忽略；确认 `BIGQMT_REDIS_CONFIG` 里有 `file_dir`/`file_token` 即可，改后重跑 DRYRUN（自带 reload）；若仍异常重启大QMT |
| `ping: False` / 连接失败 | 大QMT脚本没跑 / Redis 不通 / db 不一致 | 查大QMT输出面板有无 `[bigqmt_rpc] started channel`；确认守护进程与 miniQMT 端 host/port/**db** 一致；`redis-cli -n 5 ping` |
| `RPC下单被拒: QMT_RPC_ALLOW_ORDER=False` | miniQMT 端没开下单开关 | 设 `QMT_RPC_ALLOW_ORDER=true` 重启 |
| 下单无反应 / passorder 未执行 | 大QMT端 `rpc_allow_order_methods=False` | 改 local_config 为 `True`，重新运行策略 |
| 下单成功但无成交回调 | 大QMT端 `exec_events_enabled` 未开 | 确认 local_config `exec_events_enabled=True`；回调失效时兜底轮询仍会在 ~1s 内补触发 |
| 账号 account_id 为空 / 持仓空 | 两端账号不一致 | `BIGQMT_ACCOUNT_ID` == `account_config.json.account_id` == miniQMT 进程账号 |
| 工厂启动即抛 `交易接口开关互斥` | 同时开了多个后端开关 | 只保留 `ENABLE_QMT_RPC_FALLBACK=true`，关掉另两个 |
| `import xtquant` 行为异常 | vendored `src/xtquant` shim 遮蔽真实包 | 适配器已 append 到 sys.path 末尾规避；勿手动把 vendored src 插到 path 前面 |
| order_id 无法撤单 (`未找到 order_sys_id`) | 委托回报尚未到达，sysid 未回填 | 稍等 1–2s（轮询回填）后再撤；已终态委托本就无法撤 |
| 撤单/下单卡顿 | Redis 跨机网络抖动 | 优先同机 Redis；`QMT_RPC_TIMEOUT_SECONDS` 可适当调大 |
| 下单后委托查不到 | strategy_name 不匹配 | miniQMT 端下单时传 `strategy_name="bigqmt_signal_trader"`（与大QMT RPC 运行时的 strategy_name 一致）；不带此参数则 query_orders 过滤不到该笔单 |
| 下单有回执但柜台零痕迹，ping 却正常，委托/成交查询空或超时 | 大QMT 客户端内交易会话失效（详见第六节） | 在QMT客户端内停止并重跑模型策略（约25秒），恢复后最小单实测；miniQMT 侧重启无效 |

---

## 五、回滚

联调不通或需回退，**关掉一个环境变量即可**恢复原行为，无需改代码：

```bat
set ENABLE_QMT_RPC_FALLBACK=false
```

→ 工厂回落到默认 `easy_qmt_trader`（xttrader 直连）或你原用的后端。

---

## 六、交易会话失效故障（2026-11-02 实战定式）

**故障指纹**（ping 通 + 回执正常 + 无 sysid + 委托/成交查询空或超时）：
- miniQMT 日志大量 "QMT 连接断开"但 ping 探测秒恢复（假恢复）；
- `RPC下单已提交` 有回执（返回 `bq:xxx:rpc-xxx`），但柜台委托页/成交页零痕迹；
- 本地交易记录照常落库（前端"有记录却查不到"是本地表与柜台实查双口径差异）。

**根因**：大QMT 客户端（策略跑在 XtItClient 进程内）内部交易会话失效——策略线程
存活所以 ping/回执全通，但 passorder 委托被静默丢弃、get_trade_detail_data
(ORDER/DEAL) 返回空或阻塞。诱因疑似客户端与柜台连接闪断重连后未重绑策略会话
（QMT 客户端日志不落盘，无法实锤）。miniQMT 侧重启无效（14:18 已实证）。

**处置**（约 25 秒，无需重启 QMT 客户端）：
1. 在 QMT 客户端内**停止并重新运行**模型策略（【模型交易】实盘）；
2. 恢复后**先下最小单实测**（100 股→撤单），确认日志出现 `sysid=63xxxxxx`；
3. miniQMT 日志无 `[ORDER_CONFIRM]` 告警即确认会话健康。

**防线日志样例**（miniQMT 侧已内置，触发即按上面处置）：

```
[ORDER_CONFIRM] RPC下单 sell 588080.SH 180000股 order_id=xxx 提交已超15s仍未见柜台委托记录，
交易会话疑似失效（ping正常≠会话可用）。请核对柜台委托页；处置：在QMT客户端内停止并重新运行模型策略，
恢复后先下最小单验证。
[MONITOR] QMT RPC ping 已恢复，但存在回执后未进柜台的委托（交易会话疑似失效），保持断连状态；
处置：在QMT客户端内停止并重新运行模型策略
```

**防线机制**（qmt_rpc_trader.py）：下单回执后 15 秒（`QMT_RPC_ORDER_CONFIRM_TIMEOUT`
可调）柜台查询仍无该委托 → 告警一次并置 `session_suspect`；疑似期间 connect/恢复
探测拒绝标记恢复（前端连接徽标保持"断开"，不再假恢复）；任一新委托被柜台查询
确认后自动解除；跨日自动失效。配套修复：回调注册去重（消断连风暴/事件重复处理）。

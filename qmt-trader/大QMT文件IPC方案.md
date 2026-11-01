# 大QMT文件IPC方案 —— xttrader 降级替代

> ⚠️ **已废弃（历史文档，2026-10-31 标注）**：本方案从未实盘启用，已被**大QMT文件桥通道方案**
> （见 `大QMT文件桥通道方案.md`）取代——文件桥同样是文件交换思路，但走 req/resp JSON 帧
> + FileRelayDaemon 桥接 Redis，已实盘验证通过。部署/排障以 `大QMT-RPC联调checklist.md`
> 为准。本文仅供历史参考；对应实现（QMT_trade_executor.py / qmt_ipc_trader.py）与
> `ENABLE_QMT_IPC_FALLBACK` 开关（默认 False）保留不动，勿在新部署中启用。

> 当券商收紧 miniQMT 权限后，用大QMT内置 Python + 文件系统进程间通信（IPC）替代 xttrader 的完整方案。
> 策略端跑在自己的量化程序里，通过 JSON 文件告诉大QMT"买什么、卖什么"，大QMT负责执行。

## 一、总体架构

```
┌─────────────────────┐     文件系统目录      ┌──────────────────────┐
│  你的量化程序         │ ◄══════════════════► │  QMT内置Python脚本    │
│  (策略/信号/风控)      │     JSON文件IPC      │  (执行xttrader)      │
│  Python/C++/等        │                      │                      │
└─────────────────────┘                      └──────────────────────┘
    写订单指令 → pending/               pending/ → 读指令
    读成交回执 ← done/                  下单 → 写回执 → done/
    QMT心跳检测 ← status/heartbeat      周期写账户快照 → status/account
```

**核心逻辑**：大QMT本身自带 xttrader 授权，不开 miniQMT 也能用。只需要让大QMT内置 Python 定时跑脚本，从文件目录读你的指令来下单。

## 二、目录结构

```
C:\QuantIPC\
├── orders\                 # 指令目录
│   ├── pending\            # [策略端] 放下单文件 → QMT监听到取走
│   │   └── ord_{id}.json
│   ├── processing\         # [QMT] 正在执行中（防重复处理）
│   └── done\               # [QMT] 执行完成 + 成交回报
│       └── ord_{id}.json
│
├── status\                 # QMT定时写账户状态
│   ├── account.json        # 资金/持仓快照
│   └── heartbeat.json      # QMT心跳（活着/断线检测用）
│
├── cancel\                 # 撤单指令
│   └── cancel_{ord_id}.json
│
├── qmt_log\                # QMT侧运行日志
│   └── log_2025XXXX.txt
│
└── config.json             # 双方共用的配置文件（可选）
```

## 三、文件协议

### 3.1 下单指令（策略端 → `orders/pending/`）

```json
{
    "version": "1.0",
    "order_id": "ORD_20250706_001234",
    "timestamp": "2025-07-06 14:30:00.123",
    "action": "buy",
    "stock_code": "000001.SZ",
    "price_type": "limit",
    "price": 10.50,
    "volume": 1000,
    "strategy": "mean_reversion",
    "timeout_sec": 60,
    "remark": "日线突破信号"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `order_id` | string | **全局唯一**，策略端生成。QMT回执里原样带回，用于配对 |
| `action` | `buy` / `sell` | 买卖方向 |
| `price_type` | `limit` / `market` | 限价单/市价单 |
| `price` | float | 限价单必填，市价单传 0 |
| `volume` | int | 股数（100的整数倍） |
| `timeout_sec` | int | 超时自动撤单，0=不撤 |
| `strategy` | string | 来源策略标记，方便复盘 |

### 3.2 成交回执（QMT → `orders/done/`）

```json
{
    "version": "1.0",
    "order_id": "ORD_20250706_001234",
    "orig_timestamp": "2025-07-06 14:30:00.123",
    "exec_timestamp": "2025-07-06 14:30:02.456",
    "status": "filled",
    "filled_price": 10.48,
    "filled_volume": 1000,
    "total_volume": 1000,
    "entrust_id": "S1234567890",
    "error": null,
    "qmt_session_id": "QMT_789"
}
```

`status` 可能值：

| 值 | 含义 |
|----|------|
| `filled` | 全部成交 |
| `partial` | 部分成交（见 `filled_volume`） |
| `rejected` | 被柜台拒绝（`error` 字段写原因） |
| `cancelled` | 已撤单（超时或主动撤） |
| `pending` | 已报待成（少见，大单分笔） |

### 3.3 撤单指令（策略端 → `cancel/`）

```json
{
    "version": "1.0",
    "cancel_id": "CAN_20250706_001234",
    "order_id": "ORD_20250706_001234",
    "timestamp": "2025-07-06 14:31:00.000",
    "reason": "signal_cancelled"
}
```

### 3.4 账户快照（QMT定时写 → `status/account.json`）

```json
{
    "timestamp": "2025-07-06 14:30:00",
    "total_asset": 1000000.00,
    "available": 800000.00,
    "market_value": 200000.00,
    "frozen": 5000.00,
    "positions": [
        {"stock": "000001.SZ", "volume": 5000, "cost": 10.20, "market_price": 10.48},
        {"stock": "600519.SH", "volume": 200, "cost": 1500.00, "market_price": 1520.00}
    ],
    "today_pnl": 2500.00
}
```

当前 executor 写快照时会做最小清洗：过滤 `volume<=0` 的伪持仓；休盘时若 `market_price=0` 且 `market_value>0`，按 `market_value / volume` 反推参考价；若 `total_asset<=0` 但有持仓市值，则以 `available + market_value` 兜底，避免策略端和 Web 误判资产清零。

## 四、QMT侧代码

这段代码跑在**大QMT内置Python环境**里。早期方案要求在QMT策略编辑器中设置“定时运行模式，周期 1000ms”；光大金阳光 QMT 实测界面可能只提供“模型交易”入口。当前正式版 `QMT_trade_executor.py` 在模型交易 `init(ContextInfo)` 中进入前台循环，使用“模型交易/实盘/运行”也能自行按秒轮询 IPC，不再强依赖界面里的“定时运行”菜单。

```python
# QMT_trade_executor.py — 放在 QMT 的 Python 脚本目录，设为定时运行（1000ms）
import os
import json
import time
import shutil
import traceback
from datetime import datetime

IPC_ROOT = r"C:\QuantIPC"
DIR_PENDING = os.path.join(IPC_ROOT, "orders", "pending")
DIR_PROCESSING = os.path.join(IPC_ROOT, "orders", "processing")
DIR_DONE = os.path.join(IPC_ROOT, "orders", "done")
DIR_CANCEL = os.path.join(IPC_ROOT, "cancel")
DIR_STATUS = os.path.join(IPC_ROOT, "status")
DIR_LOG = os.path.join(IPC_ROOT, "qmt_log")
ACCOUNT_ID = "你的资金账号"

# 确保目录存在
for d in [DIR_PENDING, DIR_PROCESSING, DIR_DONE, DIR_CANCEL, DIR_STATUS, DIR_LOG]:
    os.makedirs(d, exist_ok=True)

def log(msg):
    path = os.path.join(DIR_LOG, f"log_{datetime.now().strftime('%Y%m%d')}.txt")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S.%f')[:12]}] {msg}\n")

def get_trader():
    """懒加载xttrader连接"""
    if not hasattr(get_trader, "_trader") or get_trader._trader is None:
        try:
            from xtquant import xttrader
            trader = xttrader.XtQuantTrader(
                mini_qmt_path=r"C:\国金证券QMT\bin",  # ← 改成你的QMT安装目录
                session_id=int(time.time()) % 100000
            )
            trader.connect()
            trader.login(ACCOUNT_ID)
            get_trader._trader = trader
            log(f"xttrader 连接成功")
        except Exception as e:
            log(f"xttrader 连接失败: {e}")
            return None
    return get_trader._trader

def write_result(processing_path, order, status, msg, filled_price=0, filled_vol=0):
    """写成交回执"""
    result = {
        "version": "1.0",
        "order_id": order["order_id"],
        "orig_timestamp": order.get("timestamp", ""),
        "exec_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
        "status": status,
        "filled_price": filled_price,
        "filled_volume": filled_vol,
        "total_volume": order["volume"],
        "error": msg if status in ("rejected", "error", "cancelled_by_user") else None,
    }
    done_path = os.path.join(DIR_DONE, os.path.basename(processing_path))
    with open(done_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    try:
        os.remove(processing_path)
    except:
        pass

def process_order(filepath):
    """处理一个下单文件"""
    filename = os.path.basename(filepath)
    order = None
    try:
        # 原子取走文件：pending → processing（rename是NTFS原子操作）
        processing_path = os.path.join(DIR_PROCESSING, filename)
        shutil.move(filepath, processing_path)

        with open(processing_path, "r", encoding="utf-8") as f:
            order = json.load(f)

        order_id = order["order_id"]
        log(f"处理 {order_id}: {order['action']} {order['stock_code']} {order['volume']}股")

        # 检查是否被撤单
        cancel_path = os.path.join(DIR_CANCEL, f"cancel_{order_id}.json")
        if os.path.exists(cancel_path):
            write_result(processing_path, order, "cancelled", "用户撤单")
            os.remove(cancel_path)
            return

        trader = get_trader()
        if trader is None:
            write_result(processing_path, order, "rejected", "xttrader未连接")
            return

        from xtquant import xtconstant
        xt_action = xtconstant.STOCK_BUY if order["action"] == "buy" else xtconstant.STOCK_SELL

        if order.get("price_type") == "market":
            seq = trader.order_stock(ACCOUNT_ID, order["stock_code"], xt_action,
                                     order["volume"], xtconstant.FIX_PRICE, 0, xtconstant.LATEST_PRICE)
        else:
            seq = trader.order_stock(ACCOUNT_ID, order["stock_code"], xt_action,
                                     order["volume"], xtconstant.FIX_PRICE, order["price"])

        log(f"委托序号={seq}")

        # 轮询等待成交（最多 timeout_sec 秒）
        timeout = order.get("timeout_sec", 30)
        deadline = time.time() + timeout
        result = None

        while time.time() < deadline:
            time.sleep(0.5)
            orders = trader.query_all_orders(ACCOUNT_ID)
            for o in orders:
                if str(o.order_id) == str(seq):
                    if o.order_status == xtconstant.ORDER_ALL_TRADED:
                        result = {"status": "filled", "price": o.traded_price, "vol": o.traded_vol}
                    elif o.order_status in (xtconstant.ORDER_PART_TRADED, xtconstant.ORDER_PART_TRADED_CANCELED):
                        result = {"status": "partial", "price": o.traded_price, "vol": o.traded_vol}
                    elif o.order_status == xtconstant.ORDER_CANCELED:
                        result = {"status": "cancelled", "price": 0, "vol": 0}
                    break
            if result:
                break

        if result is None:
            trader.cancel_order(ACCOUNT_ID, seq)
            result = {"status": "cancelled_timeout", "price": 0, "vol": 0}
            log(f"订单 {order_id} 超时({timeout}秒)已撤单")

        write_result(processing_path, order, result["status"],
                     f"成交价={result['price']}, 量={result['vol']}",
                     filled_price=result["price"], filled_vol=result["vol"])

    except Exception as e:
        log(f"异常: {e}\n{traceback.format_exc()}")
        if order:
            write_result(processing_path, order, "error", str(e))

def update_account_status():
    """写账户持仓快照"""
    try:
        trader = get_trader()
        if not trader:
            return
        account = trader.query_account(ACCOUNT_ID)
        positions = trader.query_stock_positions(ACCOUNT_ID)
        snapshot = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_asset": account.total_asset,
            "available": account.available,
            "market_value": account.market_value,
            "frozen": account.frozen_cash,
            "positions": [{"stock": p.stock_code, "volume": p.volume,
                           "cost": p.open_price, "market_price": p.market_price}
                          for p in positions],
            "today_pnl": account.pnl
        }
        with open(os.path.join(DIR_STATUS, "account.json"), "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
    except:
        pass

def main():
    """QMT periodic entry."""
    # 心跳
    with open(os.path.join(DIR_STATUS, "heartbeat.json"), "w") as f:
        json.dump({"ts": datetime.now().isoformat()}, f)

    # 处理待下单文件
    pending = sorted([f for f in os.listdir(DIR_PENDING) if f.endswith(".json") and not f.startswith(".")])
    for filename in pending[:5]:  # 每轮最多处理5个
        filepath = os.path.join(DIR_PENDING, filename)
        if os.path.exists(filepath):
            process_order(filepath)

    # 每30次更新账户状态
    if not hasattr(main, "_counter"):
        main._counter = 0
    main._counter += 1
    if main._counter % 30 == 0:
        update_account_status()

if __name__ == "__main__":
    main()
```

## 五、策略端调用代码

```python
# your_strategy.py — 你的量化策略，直接调这些函数
import os
import json
import time
import uuid
from datetime import datetime

IPC_ROOT = r"C:\QuantIPC"

def send_order(action, stock_code, volume, price=None, price_type="limit",
               strategy="default", timeout_sec=30):
    """
    发送下单指令 → QMT执行
    返回 order_id（全局唯一）
    """
    order_id = f"ORD_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    order = {
        "version": "1.0",
        "order_id": order_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
        "action": action,
        "stock_code": stock_code,
        "price_type": price_type if price_type else "market",
        "price": price if price else 0,
        "volume": volume,
        "strategy": strategy,
        "timeout_sec": timeout_sec,
        "remark": ""
    }
    # 先写.tmp再rename → 原子写入，防止QMT读到半截文件
    tmp = os.path.join(IPC_ROOT, "orders", "pending", f"_{order_id}.tmp")
    final = os.path.join(IPC_ROOT, "orders", "pending", f"{order_id}.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(order, f, ensure_ascii=False, indent=2)
    os.rename(tmp, final)  # NTFS原子操作
    return order_id

def wait_result(order_id, timeout=60, poll_interval=0.2):
    """等待成交回执，返回 dict 或 None"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for f in os.listdir(os.path.join(IPC_ROOT, "orders", "done")):
            if order_id in f:
                with open(os.path.join(IPC_ROOT, "orders", "done", f), "r") as fh:
                    return json.load(fh)
        time.sleep(poll_interval)
    return None

def cancel_order(order_id):
    """发送撤单指令"""
    cancel = {
        "version": "1.0",
        "cancel_id": f"CAN_{order_id}",
        "order_id": order_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
        "reason": "strategy_cancel"
    }
    with open(os.path.join(IPC_ROOT, "cancel", f"cancel_{order_id}.json"), "w") as f:
        json.dump(cancel, f, ensure_ascii=False, indent=2)

def is_qmt_alive(max_age=10):
    """检查QMT是否在线（心跳文件不超过 max_age 秒）"""
    path = os.path.join(IPC_ROOT, "status", "heartbeat.json")
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) < max_age

def get_account():
    """读取账户快照"""
    path = os.path.join(IPC_ROOT, "status", "account.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None

# ======= 使用示例 =======
if __name__ == "__main__":
    if not is_qmt_alive():
        print("QMT不在线！")
        exit(1)

    oid = send_order("buy", "000001.SZ", 1000, price=10.50, strategy="demo")
    print(f"已发单: {oid}")

    result = wait_result(oid, timeout=60)
    if result:
        print(f"成交: {result['status']}, 价={result['filled_price']}, 量={result['filled_volume']}")
    else:
        print("超时，请检查订单状态")
```

## 六、部署步骤

### QMT端

1. **放脚本**：把 `QMT_trade_executor.py` 放到QMT策略编辑器能访问到的目录（如 `user_define/`）
2. **设置运行方式**：
   - 打开 QMT → 模型交易/模型研究 → 新建 Python “模型交易”
   - 脚本路径选 `QMT_trade_executor.py`
   - 运行模式选“实盘”，刷新间隔可保持 1 秒
   - 不要勾选“启动本地 python”
   - 编译、保存、运行后，日志应出现 `foreground loop started by init` 和持续递增的 `tick ok accounts=...`
   - 若你的 QMT 版本确实有“定时运行/定时任务”，也可以使用，周期设 **1000ms**
3. **改路径**：脚本中 `mini_qmt_path` 改成你的QMT实际安装目录（大QMT的bin目录）
4. **改账号**：`ACCOUNT_ID` 改成你的资金账号

### 策略端

- 把你的量化策略程序里所有 `xttrader.下单()` 调用替换成 `send_order()` + `wait_result()`
- 行情数据走其他来源（AKShare、Tushare、通达信本地数据等），不再依赖 xtdata

## 七、方案对比

| 方案 | 延迟 | 可靠性 | 开发量 | 券商依赖 |
|------|------|--------|--------|---------|
| miniQMT直连 | 10-50ms | 中 | 低 | **高（被收紧）** |
| **本方案（大QMT文件IPC）** | **1-2s** | **高** | **低** | **无（大QMT本身就是客户端）** |
| PTrade API | 100ms | 高 | 中 | 高（仅部分券商） |
| easytrader（模拟点击） | 2-5s | 低 | 中 | 无 |
| 自建CTP/XTP | 5ms | 高 | 极高 | 高 |

**延迟分析**：从策略发信号 → QMT执行，全程约 1-2 秒。对中低频策略（日频/小时频/分钟频）完全够用。

## 八、关键注意事项

### 8.1 大QMT能力探索结论

截至 2026-07-12 实机调试，模型交易模式下：

- `get_trade_detail_data`、`passorder`、`cancel` 注入在 `globals()`，不在 `ContextInfo`。
- `ContextInfo` 主要是行情、订阅、画图、回测上下文能力；未发现 `query_stock_asset`、`order_stock`、`cancel_order` 等交易方法。
- 只读快照和下单优先走 `xttrader + StockAccount`；VBA 读取作为 fallback。
- 市价单不能把 `LATEST_PRICE` 作为额外参数传入，应使用 `price_type=LATEST_PRICE, price=0`。

### 8.2 长期运行注意事项

1. **QMT脚本重启后状态不丢**：每次重新扫 `pending/` 目录，`processing/` 里残留的文件由 `recover_stale_processing()` 自动回收（超过 `QMT_IPC_PROCESSING_STALE_SEC` 默认120秒写 `error` 回执并清理，**绝不重发防重复下单**），策略端不再空等
2. **文件写半截保护（双向）**：策略端 `.tmp → rename` 原子写 `pending/`；大QMT端 `done/`、`status/account.json`、`status/heartbeat.json` 也统一走 `_atomic_write_json()`（`.tmp → os.replace`），杜绝任一方向读到不完整 JSON（此前 account.json 半截读会被解析为空 → 策略端误判持仓清零）
3. **QMT执行超时**：QMT的定时运行每次约30秒限制，代码里限制每轮最多5个订单
4. **QMT崩溃检测**：策略端 `is_qmt_alive()` / `QmtIpcTrader.ping_xttrader()` 检查心跳，离线时发通知/报警；**下单前也做心跳门禁**，大QMT离线时 `buy/sell` 立即失败不阻塞 `QMT_IPC_ORDER_TIMEOUT` 秒
5. **权限问题**：策略程序如果跑在不同用户下，给 `C:\QuantIPC\` 目录 Everyone Read/Write 权限
6. **盘中撤单**：委托存活期间策略端写 `cancel/cancel_{id}.json`，executor 轮询时检测到即向柜台请求撤单
7. **done 目录膨胀**：`archive_old_done()` 把超过 `QMT_IPC_DONE_RETENTION_SEC`（默认1天）的回执移入 `orders/done_archive/`
8. **配置注入**：executor 侧 `IPC_ROOT` / `QMT_IPC_ACCOUNT_FILTER` / `QMT_IPC_PROCESSING_STALE_SEC` / `QMT_IPC_DONE_RETENTION_SEC` 均可用环境变量覆盖，免改脚本
9. **同步/异步兼容**：`QmtIpcTrader` 下单同步返回真实 `order_id`，并把 `order_id` 自映射写入 `order_id_map`，因此无论 `config.USE_SYNC_ORDER_API` 取 True/False 都能正常配对（不必手改该开关）
10. **模型交易非交易日可能只触发一次**：如果日志只有一次 `tick ok ... ticks=1`，不是交易 API 卡住，而是旧版仍依赖 QMT 继续调用 `handlebar()` 或依赖后台线程。正式版 executor 在 `init()` 中进入前台阻塞循环；排障时优先看 `heartbeat.json` 修改时间和日志里的 `foreground loop started by init`。
11. **不要把后台线程当作主保活方案**：实测大QMT策略容器可能在回调返回后清理脚本后台线程，即使 `daemon=False` 也不可靠。top-level worker 仅保留给本地导入、定时运行或特殊调试兜底；模型交易模式应只保留 `init()` 前台循环。
12. **旧实例会造成文件锁和重复快照**：如果日志继续出现 `worker start requested by top-level`、旧格式 `heartbeat.json.<pid>.tmp`、`WinError 32` 或同一账号短时间多次 `snapshot xttrader`，通常是旧策略实例未停止干净。先停止策略，必要时重启大QMT客户端，再运行最新版脚本。

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
L2 — 外部交易接口端到端测试（buy_single / sell / cancel / account）

打通四个新增 Web API 的完整链路，不 mock 中间层：

    web_server.buy_single / sell_single
      → _format_stock_code_for_trade      代码格式化（模拟模式补后缀）
      → trading_strategy.manual_buy/sell  ENABLE_ALLOW_BUY/SELL 门控 + M_simu 标识
      → trading_executor.buy_stock/sell_stock
                                         模拟旁路（跳过 qmt_trader / 无视交易时间）
    web_server.cancel_order
      → position_manager._cancel_order    int 转换 + 3 次重试（此处 patch 验证传参）
    web_server.account_info
      → position_manager.get_account_info 模拟账户资产视图

隔离：import web_server 前把 xtquant 系模块塞成 MagicMock；
只写 data/trading_test.db 与 :memory:，不发真实 HTTP 到 5000/5001/8888。
显式传 price 走买入/卖出链路，不依赖 xtdata 自动取价。
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("MINIQMT_DISABLE_DOTENV", "1")

# ---------------------------------------------------------------------------
# import web_server 之前临时把迅投库替换成 MagicMock，避免首次导入时
# 触碰真实行情/交易服务；import 完成后立刻还原。
#
# ⚠️ 必须在模块顶层就还原，不能拖到 tearDownModule：
# run_integration_regression_tests.py 会先 __import__ 全部测试模块、
# 之后才开始跑用例。若在此期间把 sys.modules['easy_qmt_trader'] 留成
# MagicMock，其他模块（如 test_trader_callback 的
# `patch("easy_qmt_trader.XtQuantTrader")`）就会 patch 到 Mock 上，
# 导致跨模块串扰。
# ---------------------------------------------------------------------------
_MOCKED_MODULE_NAMES = [
    'xtquant', 'xtquant.xtdata', 'xtquant.xttrader', 'xtquant.xttype',
    'easy_qmt_trader',
]
_orig_sys_modules = {k: sys.modules[k] for k in _MOCKED_MODULE_NAMES
                    if k in sys.modules}
for _mod in _MOCKED_MODULE_NAMES:
    sys.modules[_mod] = MagicMock()

try:
    import config

    config.ENABLE_SIMULATION_MODE = True
    config.DB_PATH = "data/trading_test.db"

    import web_server
    from position_manager import PositionManager
    from trading_executor import TradingExecutor
    from strategy import TradingStrategy
finally:
    for _name in _MOCKED_MODULE_NAMES:
        if _name in _orig_sys_modules:
            sys.modules[_name] = _orig_sys_modules[_name]
        else:
            sys.modules.pop(_name, None)


class TestManualTradeEndpoints(unittest.TestCase):
    """外部交易接口：买入 / 卖出 / 撤单 / 账户"""

    @classmethod
    def setUpClass(cls):
        cls._orig_db_path = config.DB_PATH
        cls._orig_token = config.WEB_API_TOKEN
        # 关闭 token 校验，本用例聚焦交易链路而非鉴权
        config.WEB_API_TOKEN = ''
        web_server.app.config['TESTING'] = True

    @classmethod
    def tearDownClass(cls):
        config.DB_PATH = cls._orig_db_path
        config.WEB_API_TOKEN = cls._orig_token

    def setUp(self):
        self._orig = {
            'balance': config.SIMULATION_BALANCE,
            'sim_mode': config.ENABLE_SIMULATION_MODE,
            'allow_buy': config.ENABLE_ALLOW_BUY,
            'allow_sell': config.ENABLE_ALLOW_SELL,
        }
        config.SIMULATION_BALANCE = 100000.0
        config.ENABLE_SIMULATION_MODE = True
        config.ENABLE_ALLOW_BUY = True
        config.ENABLE_ALLOW_SELL = True

        # 真实三件套串联，只把 data_manager 换成 Mock
        self.pm = PositionManager()
        self.pm.stop_sync_thread()

        mock_dm = MagicMock()
        mock_dm.conn = self.pm.data_manager.conn
        mock_dm.get_stock_name.return_value = '测试股票'
        mock_dm.get_latest_data.return_value = {'lastPrice': 10.0, 'lastClose': 9.8}
        mock_dm.ensure_subscribed = MagicMock()
        self.pm.data_manager = mock_dm
        self.mock_dm = mock_dm

        self.executor = TradingExecutor()
        self.executor.position_manager = self.pm
        self.executor.data_manager = mock_dm

        with patch('strategy.get_data_manager', return_value=mock_dm), \
             patch('strategy.get_indicator_calculator', return_value=MagicMock()), \
             patch('strategy.get_position_manager', return_value=self.pm), \
             patch('strategy.get_trading_executor', return_value=self.executor):
            self.strategy = TradingStrategy()
        self.strategy.position_manager = self.pm
        self.strategy.trading_executor = self.executor

        # 注入到 web_server（trading_strategy 是模块级全局，路由直接引用）
        self._orig_ws_data_manager = web_server.data_manager
        self._orig_ws_executor = web_server.trading_executor
        self._orig_ws_strategy = web_server.trading_strategy
        web_server.data_manager = mock_dm
        web_server.trading_executor = self.executor
        web_server.trading_strategy = self.strategy
        web_server.set_position_manager(self.pm)

        self._clear_tables()
        self.client = web_server.app.test_client()

    def tearDown(self):
        web_server.data_manager = self._orig_ws_data_manager
        web_server.trading_executor = self._orig_ws_executor
        web_server.trading_strategy = self._orig_ws_strategy
        web_server.set_position_manager(None)
        try:
            self.pm.stop_sync_thread()
        except Exception:
            pass
        config.SIMULATION_BALANCE = self._orig['balance']
        config.ENABLE_SIMULATION_MODE = self._orig['sim_mode']
        config.ENABLE_ALLOW_BUY = self._orig['allow_buy']
        config.ENABLE_ALLOW_SELL = self._orig['allow_sell']

    # ---------- 工具 ----------

    def _clear_tables(self):
        with self.pm.memory_conn_lock:
            self.pm.memory_conn.execute("DELETE FROM positions")
            self.pm.memory_conn.commit()
        cur = self.pm.conn.cursor()
        cur.execute("DELETE FROM trade_records")
        self.pm.conn.commit()

    def _buy_single(self, **payload):
        return self.client.post('/api/actions/buy_single', json=payload)

    def _sell(self, **payload):
        return self.client.post('/api/actions/sell', json=payload)

    def _position(self, stock_code):
        with self.pm.memory_conn_lock:
            cur = self.pm.memory_conn.cursor()
            cur.execute("SELECT * FROM positions WHERE stock_code=?", (stock_code,))
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

    def _trade_records(self, stock_code, trade_type):
        cur = self.pm.conn.cursor()
        cur.execute(
            "SELECT * FROM trade_records WHERE stock_code=? AND trade_type=? ORDER BY id",
            (stock_code, trade_type))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    # ---------- buy_single ----------

    def test_buy_01_volume_form_full_chain(self):
        """buy_01 volume 形式全链路：HTTP → 模拟持仓落地 + 交易流水"""
        resp = self._buy_single(stock_code='000001.SZ', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'success')
        self.assertIsNotNone(data['order_id'])
        self.assertEqual(data['stock_code'], '000001.SZ')

        pos = self._position('000001.SZ')
        self.assertIsNotNone(pos, "持仓应落地到内存库")
        self.assertEqual(int(float(pos['volume'])), 100)

        records = self._trade_records('000001.SZ', 'BUY')
        self.assertEqual(len(records), 1, "应写入一笔买入流水")

    def test_buy_02_amount_form_converts_to_volume(self):
        """buy_02 amount 形式按金额换算股数（1000 元 @10.0 → 100 股）"""
        resp = self._buy_single(stock_code='600000.SH', amount=1000, price=10.0)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'success')

        pos = self._position('600000.SH')
        self.assertIsNotNone(pos)
        self.assertEqual(int(float(pos['volume'])), 100)

    def test_buy_03_bare_code_suffixed_in_simulation(self):
        """buy_03 裸 6 位代码在模拟模式下补全市场后缀"""
        resp = self._buy_single(stock_code='000001', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['stock_code'], '000001.SZ')
        self.assertIsNotNone(self._position('000001.SZ'))

    def test_buy_04_missing_stock_code_rejected(self):
        """buy_04 缺 stock_code 返回 400"""
        resp = self._buy_single(volume=100)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()['status'], 'error')

    def test_buy_05_volume_and_amount_both_missing_rejected(self):
        """buy_05 volume 与 amount 同时缺失返回 400"""
        resp = self._buy_single(stock_code='000001.SZ')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()['status'], 'error')

    def test_buy_06_non_numeric_volume_rejected(self):
        """buy_06 volume 非数字返回 400"""
        resp = self._buy_single(stock_code='000001.SZ', volume='abc')
        self.assertEqual(resp.status_code, 400)
        self.assertIn('volume', resp.get_json()['message'])

    def test_buy_07_allow_buy_disabled_returns_failed(self):
        """buy_07 ENABLE_ALLOW_BUY=False 时业务拦截：200 + status=failed，不建仓"""
        config.ENABLE_ALLOW_BUY = False
        balance_before = config.SIMULATION_BALANCE

        resp = self._buy_single(stock_code='000001.SZ', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'failed')
        self.assertIsNone(data['order_id'])
        self.assertIsNone(self._position('000001.SZ'), "不应建仓")
        self.assertAlmostEqual(config.SIMULATION_BALANCE, balance_before, places=6,
                               msg="被拦截时资金不应变动")

    def test_buy_08_price_type_limit_passed_through(self):
        """buy_08 price_type=11(限价) 经 manual_buy 透传到 buy_stock"""
        with patch.object(self.executor, 'buy_stock', return_value=12345) as m:
            resp = self._buy_single(stock_code='000001.SZ', volume=100,
                                    price=10.0, price_type=11)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'success')
        self.assertEqual(m.call_args.kwargs['price_type'], 11)

    def test_buy_09_price_type_missing_defaults_to_5(self):
        """buy_09 不传 price_type 时缺省为 5（最新价，存量调用方行为不变）"""
        with patch.object(self.executor, 'buy_stock', return_value=12345) as m:
            resp = self._buy_single(stock_code='000001.SZ', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_args.kwargs['price_type'], 5)

    def test_buy_10_invalid_price_type_rejected(self):
        """buy_10 白名单外 price_type=999 返回 400，不直通柜台"""
        resp = self._buy_single(stock_code='000001.SZ', volume=100,
                                price=10.0, price_type=999)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('price_type', resp.get_json()['message'])

    def test_buy_11_limit_without_price_rejected(self):
        """buy_11 限价单(11)不带 price 返回 400"""
        resp = self._buy_single(stock_code='000001.SZ', volume=100, price_type=11)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('限价单', resp.get_json()['message'])

    def test_buy_12_fractional_volume_rejected(self):
        """buy_12 非整数 volume（100.7）返回 400 而非静默截断（P2-8）"""
        resp = self._buy_single(stock_code='000001.SZ', volume=100.7, price=10.0)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('volume', resp.get_json()['message'])

    def test_buy_13_boolean_volume_rejected(self):
        """buy_13 布尔 volume 返回 400（P2-8：bool 是 int 子类须显式拒绝）"""
        resp = self._buy_single(stock_code='000001.SZ', volume=True)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('volume', resp.get_json()['message'])

    def test_buy_14_infinite_volume_rejected(self):
        """buy_14 Infinity volume 返回 400 而非 int(inf) 溢出 500（P2-8）"""
        resp = self._buy_single(stock_code='000001.SZ', volume=float('inf'))
        self.assertEqual(resp.status_code, 400)
        self.assertIn('volume', resp.get_json()['message'])

    # ---------- sell ----------

    def test_sell_01_volume_form_reduces_position(self):
        """sell_01 指定数量卖出：持仓减量 + SELL 流水 + manual_simu 标识"""
        self._buy_single(stock_code='000001.SZ', volume=200, price=10.0)

        resp = self._sell(stock_code='000001.SZ', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'success')
        self.assertIsNotNone(data['order_id'])

        pos = self._position('000001.SZ')
        self.assertIsNotNone(pos)
        self.assertEqual(int(float(pos['volume'])), 100, "卖出后应剩 100 股")

        records = self._trade_records('000001.SZ', 'SELL')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['strategy'], 'manual_simu')

    def test_sell_02_ratio_form_half_position(self):
        """sell_02 ratio=0.5 卖半仓：200 股 → 卖出 100 股"""
        self._buy_single(stock_code='000001.SZ', volume=200, price=10.0)

        resp = self._sell(stock_code='000001.SZ', ratio=0.5, price=10.0)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'success')

        pos = self._position('000001.SZ')
        self.assertEqual(int(float(pos['volume'])), 100, "半仓卖出后应剩 100 股")

    def test_sell_03_not_held_in_simulation_existing_lenient_behavior(self):
        """sell_03 模拟模式卖出未持有股票的既有宽松行为（固化现状）

        sell_stock 模拟分支不校验持仓：_update_position_after_trade 找不到
        持仓仅告警，仍生成 SELL 流水并给模拟账户加钱；实盘分支由
        check_stock_is_av_sell 拦截。若未来收紧模拟校验，需同步更新本用例。
        """
        balance_before = config.SIMULATION_BALANCE
        resp = self._sell(stock_code='600000.SH', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'success',
                         "模拟模式既有行为：卖出未持有也会成功落账")
        self.assertIsNotNone(data['order_id'])
        self.assertEqual(len(self._trade_records('600000.SH', 'SELL')), 1,
                         "模拟模式应生成 SELL 流水")
        self.assertGreater(config.SIMULATION_BALANCE, balance_before,
                           "模拟模式既有行为：卖出未持有会给账户加钱")

    def test_sell_04_volume_and_ratio_both_missing_rejected(self):
        """sell_04 volume 与 ratio 同时缺失返回 400"""
        resp = self._sell(stock_code='000001.SZ')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()['status'], 'error')

    def test_sell_05_allow_sell_disabled_returns_failed(self):
        """sell_05 ENABLE_ALLOW_SELL=False 时 manual_sell 层拦截"""
        self._buy_single(stock_code='000001.SZ', volume=100, price=10.0)
        config.ENABLE_ALLOW_SELL = False

        resp = self._sell(stock_code='000001.SZ', volume=100, price=10.0)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'failed')
        pos = self._position('000001.SZ')
        self.assertIsNotNone(pos, "拦截时持仓不应变动")
        self.assertEqual(int(float(pos['volume'])), 100)

    def test_sell_06_price_type_passed_through(self):
        """sell_06 price_type=44(对手方最优) 经 manual_sell 透传到 sell_stock"""
        with patch.object(self.executor, 'sell_stock', return_value=23456) as m:
            resp = self._sell(stock_code='000001.SZ', volume=100,
                              price=10.0, price_type=44)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'success')
        self.assertEqual(m.call_args.kwargs['price_type'], 44)

    # ---------- cancel ----------

    def test_cancel_01_delegates_with_int_order_id(self):
        """cancel_01 委托号透传 _cancel_order 并做 int 转换"""
        with patch.object(self.pm, '_cancel_order', return_value=True) as m:
            resp = self.client.post('/api/orders/cancel',
                                    json={'order_id': '12345', 'stock_code': '000001.SZ'})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'success')
        m.assert_called_once_with('000001.SZ', 12345)

    def test_cancel_02_failure_returns_failed(self):
        """cancel_02 撤单失败（如模拟模式无在途委托）返回 status=failed"""
        with patch.object(self.pm, '_cancel_order', return_value=False):
            resp = self.client.post('/api/orders/cancel', json={'order_id': 999})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['status'], 'failed')

    def test_cancel_03_missing_order_id_rejected(self):
        """cancel_03 缺 order_id 返回 400"""
        resp = self.client.post('/api/orders/cancel', json={})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()['status'], 'error')

    def test_cancel_04_non_integer_order_id_rejected(self):
        """cancel_04 非整数 order_id 返回 400"""
        resp = self.client.post('/api/orders/cancel', json={'order_id': 'abc'})
        self.assertEqual(resp.status_code, 400)
        self.assertIn('order_id', resp.get_json()['message'])

    # ---------- account ----------

    def test_account_01_simulation_account_info(self):
        """account_01 模拟模式返回资产视图 + simulation_mode 标记"""
        self._buy_single(stock_code='000001.SZ', volume=100, price=10.0)

        resp = self.client.get('/api/account')

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'success')
        self.assertTrue(data['simulation_mode'])

        info = data['data']
        self.assertAlmostEqual(info['available'], config.SIMULATION_BALANCE, places=2,
                               msg="可用资金应等于当前模拟余额")
        self.assertGreater(info['market_value'], 0, "持仓市值应大于 0")
        self.assertAlmostEqual(info['total_asset'],
                                info['available'] + info['market_value'], places=2)
        self.assertTrue(info.get('account_id'), "应携带 account_id")

    def test_account_02_unavailable_returns_503(self):
        """account_02 账户信息不可用时返回 503 而非 500"""
        with patch.object(self.pm, 'get_account_info', return_value=None):
            resp = self.client.get('/api/account')

        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.get_json()['status'], 'error')


if __name__ == '__main__':
    unittest.main(verbosity=2)

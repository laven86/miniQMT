"""
历史数据源路由回归测试

经验背景（防止再次踩坑）:
  提交 68dda78a 曾把 `DataManager.download_history_data` 的数据源路由从
  「仅 ENABLE_XTQUANT_MANAGER=True 才用 xtdata」改成无条件 `if self.xt:` 优先 xtdata。
  结果标准模式(ENABLE_XTQUANT_MANAGER=False)启动「下载初始数据」时会调用
  `xtdata.get_market_data_ex(...)`，在部分 QMT 客户端触发底层 C++ 断言
  `Assertion failed: u < 1000000 (bsonobj.cpp:1388)`，直接 abort 整个进程——
  try/except 与 ThreadPoolExecutor 超时都拦不住（C++ abort 不是 Python 异常）。

  本测试锁定路由不变量：
    - 标准模式(ENABLE_XTQUANT_MANAGER=False) → 走 Mootdx，绝不触碰 xtdata 历史接口
      （尤其是会崩溃的 get_market_data_ex）
    - 网关模式(ENABLE_XTQUANT_MANAGER=True)  → 优先 xtdata，失败再 fallback 到 Mootdx

  若有人再把条件改回无条件 `if self.xt:`，下方核心防护用例会立即失败。

全部使用 Mock，不依赖真实 QMT/Mootdx 环境。
"""

import unittest
import sys
import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from test.test_base import TestBase


def _make_data_manager():
    """返回一个跳过 IO 初始化的 DataManager 实例（参照 test_xtdata_data_source）。"""
    import os as _os
    with patch('data_manager.DataManager._init_xtquant'):
        orig_dir = config.DATA_DIR
        config.DATA_DIR = tempfile.mkdtemp()
        orig_db = config.DB_PATH
        config.DB_PATH = _os.path.join(config.DATA_DIR, 'test_dm.db')
        try:
            from data_manager import DataManager
            dm = DataManager()
        finally:
            config.DATA_DIR = orig_dir
            config.DB_PATH = orig_db
    return dm


class TestHistoryDataSourceRouting(TestBase):
    """download_history_data 历史数据源路由不变量验证"""

    def setUp(self):
        super().setUp()
        self.dm = _make_data_manager()
        # 注入 mock xtdata 连接：若路由错误地走 xtdata，mock 上的调用会被记录
        self.mock_xt = MagicMock()
        self.dm.xt = self.mock_xt

    def _download(self, stock_code='003025.SZ'):
        return self.dm.download_history_data(
            stock_code, period='1d', start_date='20260323', end_date='20260621')

    # ── 柜台虚拟代码豁免：不进任何数据源 ──────────────────────────────

    def test_download_history_exempt_synthetic_repo_code(self):
        """⭐柜台虚拟代码（888880 国债逆回购聚合）直接豁免：
        不进 xtdata / Tushare / Mootdx 任何数据源（它无历史数据，
        请求只会逐级 fallback 报错）"""
        with patch('Methods.getStockData') as mock_mootdx:
            result = self.dm.download_history_data('888880')
        self.assertIsNone(result)
        self.mock_xt.download_history_data.assert_not_called()
        self.mock_xt.get_local_data.assert_not_called()
        self.mock_xt.get_market_data_ex.assert_not_called()
        mock_mootdx.assert_not_called()

    # ── 标准模式：必须走 Mootdx，绝不碰 xtdata 历史接口 ──────────────────

    def test_standard_mode_never_calls_get_market_data_ex(self):
        """⭐核心防护：ENABLE_XTQUANT_MANAGER=False 时绝不调用 get_market_data_ex
        （该接口在部分 QMT 客户端触发 BSON 断言 u<1000000 导致进程 abort）"""
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch('Methods.getStockData', return_value=None):
            self._download()
        self.mock_xt.get_market_data_ex.assert_not_called()
        self.mock_xt.download_history_data.assert_not_called()

    def test_standard_mode_skips_xtdata_history(self):
        """标准模式（开关关闭）不应进入 download_history_xtdata 路径"""
        self.dm.download_history_xtdata = MagicMock()
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch('Methods.getStockData', return_value=None):
            self._download()
        self.dm.download_history_xtdata.assert_not_called()

    def test_standard_mode_uses_mootdx(self):
        """标准模式（开关关闭）应走 Mootdx（调用 Methods.getStockData）"""
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch('Methods.getStockData', return_value=None) as mock_mootdx:
            self._download()
        mock_mootdx.assert_called_once()

    def test_standard_mode_bj_uses_tushare_when_enabled(self):
        """北交所标准模式优先 Tushare，不触碰 xtdata 或 Mootdx。"""
        fake_df = pd.DataFrame({'date': ['2026-06-20'], 'close': [10.0], 'stock_code': ['920118.BJ']})
        self.dm._download_history_tushare = MagicMock(return_value=fake_df)
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch.object(config, 'ENABLE_TUSHARE_DATA_SOURCE', True), \
             patch('Methods.getStockData') as mock_mootdx:
            result = self._download('920118')
        self.dm._download_history_tushare.assert_called_once_with(
            '920118.BJ',
            start_date='20260323',
            end_date='20260621',
        )
        self.mock_xt.get_market_data_ex.assert_not_called()
        mock_mootdx.assert_not_called()
        self.assertIs(result, fake_df)

    def test_standard_mode_bj_skips_mootdx_when_tushare_disabled(self):
        """北交所禁用 Tushare 时不应把 .BJ 代码降级给 Mootdx。"""
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch.object(config, 'ENABLE_TUSHARE_DATA_SOURCE', False), \
             patch('Methods.getStockData') as mock_mootdx:
            result = self._download('920118')
        self.mock_xt.get_market_data_ex.assert_not_called()
        self.mock_xt.download_history_data.assert_not_called()
        mock_mootdx.assert_not_called()
        self.assertIsNone(result)

    def test_standard_mode_holds_even_when_xt_available(self):
        """即便 self.xt 可用，标准模式（开关关闭）也不得优先 xtdata（防止回退到无条件 if self.xt:）"""
        self.dm.download_history_xtdata = MagicMock(
            return_value=pd.DataFrame({'date': ['2026-06-20'], 'close': [10.0]}))
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch('Methods.getStockData', return_value=None):
            self._download()
        self.dm.download_history_xtdata.assert_not_called()

    # ── 标准模式 + ENABLE_XTQUANT_HISTORY_DATA：xtdata 优先（2026-10 新增路由）──

    def test_standard_mode_with_xt_history_switch_uses_xtdata_first(self):
        """⭐开关开启时标准模式应优先 xtdata（本机 xtquant.xtdata 模块，非 HTTP 服务）"""
        fake_df = pd.DataFrame({'date': ['2026-10-08'], 'close': [10.0], 'stock_code': ['600000.SH']})
        self.dm.download_history_xtdata = MagicMock(return_value=fake_df)
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', True), \
             patch('Methods.getStockData') as mock_mootdx:
            result = self._download('600000')
        self.dm.download_history_xtdata.assert_called_once()
        mock_mootdx.assert_not_called()  # xtdata 成功则不 fallback
        self.assertIs(result, fake_df)

    def test_standard_mode_with_xt_history_switch_empty_falls_back(self):
        """开关开启但 xtdata 返回空时，仍应降级到 Mootdx 兑底（不因开关丢失兑底）"""
        self.dm.download_history_xtdata = MagicMock(return_value=pd.DataFrame())
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', True), \
             patch('Methods.getStockData', return_value=None) as mock_mootdx:
            self._download('600000')
        self.dm.download_history_xtdata.assert_called_once()
        mock_mootdx.assert_called_once()

    def test_xt_history_switch_off_keeps_old_routing(self):
        """开关关闭（默认）时维持旧行为：标准模式不碰 xtdata 历史"""
        self.dm.download_history_xtdata = MagicMock()
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False), \
             patch.object(config, 'ENABLE_XTQUANT_HISTORY_DATA', False), \
             patch('Methods.getStockData', return_value=None):
            self._download('600000')
        self.dm.download_history_xtdata.assert_not_called()

    def test_download_history_xtdata_prefers_get_local_data(self):
        """⭐核心防护：download_history_xtdata 必须用 get_local_data（纯本地读），
        绝不调用 get_market_data_ex（部分 QMT 客户端 BSON 断言 abort 风险）"""
        bars = pd.DataFrame(
            {'open': [10.0], 'high': [10.5], 'low': [9.8], 'close': [10.2],
             'volume': [1000], 'amount': [10200.0]},
            index=pd.Index(['20261008'], name='time'))
        self.mock_xt.get_local_data.return_value = {'600000.SH': bars}
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', False):
            result = self.dm.download_history_xtdata(
                '600000.SH', period='1d', start_date='20260101', end_date='20261008')
        self.mock_xt.get_local_data.assert_called_once()
        self.mock_xt.get_market_data_ex.assert_not_called()
        self.mock_xt.download_history_data.assert_called_once()
        self.assertIsNotNone(result)
        self.assertFalse(result.empty)
        # 时间索引应被展开为 time 列并归一化为 date 列（%Y-%m-%d）
        self.assertIn('date', result.columns)
        self.assertEqual(result.iloc[0]['date'], '2026-10-08')

    def test_download_history_xtdata_http_adapter_falls_back_to_ex(self):
        """XtQuantManager HTTP 适配器无 get_local_data 时回退 get_market_data_ex
        （该模式下调用发生在服务端进程，本地无 abort 风险）"""
        del self.mock_xt.get_local_data  # 模拟 HTTP 适配器无此方法
        bars = pd.DataFrame(
            {'open': [10.0], 'close': [10.2]}, index=pd.Index(['20261008'], name='time'))
        self.mock_xt.get_market_data_ex.return_value = {'600000.SH': bars}
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', True):
            result = self.dm.download_history_xtdata(
                '600000.SH', period='1d', start_date='20260101', end_date='20261008')
        self.mock_xt.get_market_data_ex.assert_called_once()
        self.assertIsNotNone(result)
        self.assertFalse(result.empty)

    # ── 网关模式：优先 xtdata，失败再 fallback ──────────────────────────

    def test_gateway_mode_uses_xtdata_history(self):
        """网关模式(ENABLE_XTQUANT_MANAGER=True)应优先调用 download_history_xtdata"""
        fake_df = pd.DataFrame({'date': ['2026-06-20'], 'close': [10.0], 'stock_code': ['003025.SZ']})
        self.dm.download_history_xtdata = MagicMock(return_value=fake_df)
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', True), \
             patch('Methods.getStockData') as mock_mootdx:
            result = self.dm.download_history_data(
                '003025.SZ', period='1d', start_date='20260323', end_date='20260621')
        self.dm.download_history_xtdata.assert_called_once()
        mock_mootdx.assert_not_called()  # xtdata 成功则不 fallback
        self.assertIsNotNone(result)

    def test_gateway_mode_empty_xtdata_falls_back_to_mootdx(self):
        """网关模式下 xtdata 返回空时应 fallback 到 Mootdx"""
        self.dm.download_history_xtdata = MagicMock(return_value=pd.DataFrame())
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', True), \
             patch('Methods.getStockData', return_value=None) as mock_mootdx:
            self._download()
        self.dm.download_history_xtdata.assert_called_once()
        mock_mootdx.assert_called_once()

    def test_gateway_mode_bj_uses_xtdata_history(self):
        """北交所网关模式仍优先走 xtdata，且传入标准 .BJ 代码。"""
        fake_df = pd.DataFrame({'date': ['2026-06-20'], 'close': [10.0], 'stock_code': ['920118.BJ']})
        self.dm.download_history_xtdata = MagicMock(return_value=fake_df)
        with patch.object(config, 'ENABLE_XTQUANT_MANAGER', True), \
             patch('Methods.getStockData') as mock_mootdx:
            result = self._download('920118')
        self.dm.download_history_xtdata.assert_called_once()
        self.assertEqual(self.dm.download_history_xtdata.call_args[0][0], '920118.BJ')
        mock_mootdx.assert_not_called()
        self.assertIs(result, fake_df)


if __name__ == '__main__':
    unittest.main(verbosity=2)

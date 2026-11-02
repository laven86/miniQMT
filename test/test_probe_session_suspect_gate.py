"""
_probe_qmt_recovered() 交易会话门控测试（防假恢复）

背景（2026-11-02 故障）：大QMT客户端内策略交易会话失效时 ping 全通
（ping 只证明策略线程活着），单探针 ping 判恢复造成 14:05:12 假恢复，
恢复窗口内下单全部蒸发（含一笔 180000 股卖出假成功）。
修复：ping 通过后还须检查 trader.session_suspect（回执后未进柜台的实证），
任一未解除则保持断连状态并给出处置指引。
"""

import os
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import position_manager
from position_manager import PositionManager


class TestProbeSessionSuspectGate(unittest.TestCase):

    def _make_pm(self, ping=True, session_suspect=False):
        pm = MagicMock()
        pm.qmt_connected = False  # 探测场景：当前处于断连状态
        pm._reconnect_lock = threading.Lock()
        pm._reconnect_in_progress = False
        trader = MagicMock()
        trader.ping_xttrader.return_value = ping
        trader.session_suspect = session_suspect
        trader.ensure_trade_push_ready.return_value = True
        pm.qmt_trader = trader
        return pm, trader

    def _probe(self, pm):
        with patch.object(position_manager.config, 'ENABLE_SIMULATION_MODE', False), \
                patch.object(position_manager.config, 'ENABLE_XTQUANT_MANAGER', False,
                             create=True):
            return PositionManager._probe_qmt_recovered(pm)

    def test_probe_blocked_when_session_suspect(self):
        """ping 恢复但 session_suspect=True → 拒绝标记恢复，不重注册回调。"""
        pm, trader = self._make_pm(ping=True, session_suspect=True)
        self.assertFalse(self._probe(pm))
        trader.register_trade_callback.assert_not_called()
        trader.register_disconnect_callback.assert_not_called()
        self.assertFalse(pm.qmt_connected)

    def test_probe_passes_when_session_healthy(self):
        """ping 恢复且无疑似实证 → 正常标记恢复并重注册回调（去重后幂等）。"""
        pm, trader = self._make_pm(ping=True, session_suspect=False)
        self.assertTrue(self._probe(pm))
        trader.register_trade_callback.assert_called_once()
        trader.register_disconnect_callback.assert_called_once()

    def test_probe_blocked_when_ping_fails(self):
        """ping 不通 → 保持原有行为返回 False。"""
        pm, trader = self._make_pm(ping=False)
        self.assertFalse(self._probe(pm))
        trader.register_trade_callback.assert_not_called()


if __name__ == '__main__':
    unittest.main()

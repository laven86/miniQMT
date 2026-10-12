"""order_utils 单元测试

委托字段的格式化与状态判定在 Flask 直连和网关两条链路上共用，
口径一旦漂移，监控端就会把"已报未成交"的挂单显示成已完成或漏掉。
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from order_utils import (
    ACTIVE_ORDER_STATUS,
    ORDER_STATUS_DESC,
    ORDER_TYPE_BUY,
    ORDER_TYPE_SELL,
    format_order_time,
    is_pending,
    sort_orders,
    status_desc,
)


class TestFormatOrderTime(unittest.TestCase):
    def test_unix_timestamp_int(self):
        ts = int(datetime(2026, 8, 1, 10, 30, 0).timestamp())
        self.assertEqual(format_order_time(ts), "2026-08-01 10:30:00")

    def test_unix_timestamp_float(self):
        ts = datetime(2026, 8, 1, 9, 31, 5).timestamp()
        self.assertEqual(format_order_time(ts), "2026-08-01 09:31:05")

    def test_numeric_string_treated_as_timestamp(self):
        ts = int(datetime(2026, 8, 1, 14, 0, 0).timestamp())
        self.assertEqual(format_order_time(str(ts)), "2026-08-01 14:00:00")

    def test_datetime_object(self):
        self.assertEqual(
            format_order_time(datetime(2026, 8, 1, 15, 0, 0)),
            "2026-08-01 15:00:00",
        )

    def test_iso_string_with_t_separator(self):
        self.assertEqual(format_order_time("2026-08-01T10:30:00"), "2026-08-01 10:30:00")

    def test_plain_datetime_string(self):
        self.assertEqual(format_order_time("2026-08-01 10:30:00"), "2026-08-01 10:30:00")

    def test_microsecond_precision_is_truncated(self):
        self.assertEqual(
            format_order_time("2026-08-01 10:30:00.123456"),
            "2026-08-01 10:30:00",
        )

    def test_none_returns_none(self):
        self.assertIsNone(format_order_time(None))

    def test_zero_and_negative_timestamp_return_none(self):
        self.assertIsNone(format_order_time(0))
        self.assertIsNone(format_order_time(-1))

    def test_empty_string_returns_none(self):
        self.assertIsNone(format_order_time(""))
        self.assertIsNone(format_order_time("   "))

    def test_unparseable_string_is_returned_as_is(self):
        self.assertEqual(format_order_time("not-a-time"), "not-a-time")

    def test_bool_is_not_treated_as_timestamp(self):
        result = format_order_time(True)
        self.assertNotIn("1970", str(result))

    # ---------- 大QMT RPC 柜台行的时间形态（2026-10-12 委托时间字段补齐） ----------

    def test_compact_datetime_int(self):
        self.assertEqual(format_order_time(20260801103000), "2026-08-01 10:30:00")

    def test_hhmmss_int_uses_today(self):
        expected = datetime.now().strftime("%Y-%m-%d ") + "09:30:01"
        self.assertEqual(format_order_time(93001), expected)

    def test_hhmmss_digit_string(self):
        expected = datetime.now().strftime("%Y-%m-%d ") + "14:47:21"
        self.assertEqual(format_order_time("144721"), expected)

    def test_seconds_of_day_when_not_hhmmss(self):
        # 69900 不是合法 HHMMSS（分位 99），按当日秒数解读为 19:25:00
        expected = datetime.now().strftime("%Y-%m-%d ") + "19:25:00"
        self.assertEqual(format_order_time(69900), expected)

    def test_midrange_int_returns_none(self):
        # 235959 与 1e9 之间无法归一（旧行为会给出 1970 年的乱值，宁可不给）
        self.assertIsNone(format_order_time(300000))


class TestOrderStatus(unittest.TestCase):
    def test_active_status_codes_are_pending(self):
        for code in (48, 49, 50, 51, 52, 55):
            self.assertTrue(is_pending(code), msg="status %d should be pending" % code)

    def test_terminal_status_codes_are_not_pending(self):
        for code in (53, 54, 56, 57):
            self.assertFalse(is_pending(code), msg="status %d should not be pending" % code)

    def test_unknown_status_is_not_pending(self):
        self.assertFalse(is_pending(999))
        self.assertFalse(is_pending(0))

    def test_active_set_matches_position_manager_convention(self):
        self.assertEqual(ACTIVE_ORDER_STATUS, frozenset({48, 49, 50, 51, 52, 55}))

    def test_status_desc_maps_known_codes(self):
        self.assertEqual(status_desc(50), "已报")
        self.assertEqual(status_desc(56), "已成")
        self.assertEqual(status_desc(57), "废单")

    def test_status_desc_prefers_qmt_status_msg(self):
        self.assertEqual(status_desc(57, "资金不足"), "资金不足")

    def test_status_desc_unknown_code(self):
        self.assertEqual(status_desc(999), "未知")

    def test_order_type_constants(self):
        self.assertEqual(ORDER_TYPE_BUY, 23)
        self.assertEqual(ORDER_TYPE_SELL, 24)

    def test_status_desc_table_covers_all_active_codes(self):
        for code in ACTIVE_ORDER_STATUS:
            self.assertIn(code, ORDER_STATUS_DESC)


class TestSortOrders(unittest.TestCase):
    def _o(self, oid, pending, t):
        return {"order_id": oid, "is_pending": pending, "order_time": t}

    def test_pending_orders_come_first(self):
        orders = [
            self._o("done", False, "2026-08-01 15:00:00"),
            self._o("live", True, "2026-08-01 09:31:00"),
        ]
        sort_orders(orders)
        self.assertEqual([o["order_id"] for o in orders], ["live", "done"])

    def test_within_group_sorted_by_time_desc(self):
        orders = [
            self._o("a", True, "2026-08-01 09:31:00"),
            self._o("b", True, "2026-08-01 14:00:00"),
            self._o("c", True, "2026-08-01 10:00:00"),
        ]
        sort_orders(orders)
        self.assertEqual([o["order_id"] for o in orders], ["b", "c", "a"])

    def test_missing_time_sorts_last_within_group(self):
        orders = [
            self._o("no-time", True, None),
            self._o("timed", True, "2026-08-01 09:31:00"),
        ]
        sort_orders(orders)
        self.assertEqual([o["order_id"] for o in orders], ["timed", "no-time"])

    def test_none_time_does_not_raise(self):
        orders = [self._o("x", False, None), self._o("y", True, None)]
        sort_orders(orders)
        self.assertEqual(orders[0]["order_id"], "y")

    def test_empty_list(self):
        orders = []
        self.assertEqual(sort_orders(orders), [])

    def test_mixed_pending_and_done_full_ordering(self):
        orders = [
            self._o("d1", False, "2026-08-01 09:00:00"),
            self._o("p1", True, "2026-08-01 10:00:00"),
            self._o("d2", False, "2026-08-01 15:00:00"),
            self._o("p2", True, "2026-08-01 14:00:00"),
        ]
        sort_orders(orders)
        self.assertEqual([o["order_id"] for o in orders], ["p2", "p1", "d2", "d1"])


if __name__ == "__main__":
    unittest.main()

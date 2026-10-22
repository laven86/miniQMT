"""委托（Order）相关的共享常量与格式化工具。

web_server.py（Flask 直连）和 xtquant_manager/server.py（网关）都要把
QMT 原始委托对象映射成前端字段，此处收口以免两边各写一份、口径漂移。
"""
from datetime import datetime

# 委托状态码 → 中文描述
ORDER_STATUS_DESC = {
    48: "未报",
    49: "待报",
    50: "已报",
    51: "已报待撤",
    52: "部成待撤",
    53: "部撤",
    54: "已撤",
    55: "部成",
    56: "已成",
    57: "废单",
}

# 活跃（在途）委托状态码：已提交但尚未终结
ACTIVE_ORDER_STATUS = frozenset({48, 49, 50, 51, 52, 55})

# 委托类型码
ORDER_TYPE_BUY = 23
ORDER_TYPE_SELL = 24


def parse_order_time(value, trading_day=None):
    """Return a timestamp only when the broker time can be parsed reliably."""
    day = trading_day or datetime.now().date()
    if isinstance(day, str):
        day = datetime.strptime(day, '%Y-%m-%d').date()
    if isinstance(value, datetime):
        return value.timestamp()
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.replace('.', '', 1).isdigit():
            number = float(text)
            digits = str(int(number))
            if len(digits) == 14 and digits[:2] in ('19', '20'):
                return datetime.strptime(digits, '%Y%m%d%H%M%S').timestamp()
            if number >= 1_000_000_000:
                return datetime.fromtimestamp(number / 1000 if number >= 1e11 else number).timestamp()
            if number <= 0:
                return None
            try:
                parsed = datetime.strptime(digits.zfill(6), '%H%M%S')
            except ValueError:
                if number >= 86400:
                    return None
                from datetime import timedelta
                return (datetime.combine(day, datetime.min.time()) + timedelta(seconds=number)).timestamp()
            return datetime.combine(day, parsed.time()).timestamp()
        normalized = text.replace('T', ' ')
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%H:%M:%S'):
            try:
                parsed = datetime.strptime(normalized, fmt)
                return (datetime.combine(day, parsed.time()) if fmt == '%H:%M:%S' else parsed).timestamp()
            except ValueError:
                continue
    except (ValueError, OverflowError, OSError):
        pass
    return None


def format_order_time(value):
    """格式化委托时间为 'YYYY-MM-DD HH:MM:SS'。

    QMT 的 order_time 是 Unix 秒级时间戳(int)，不能按字符串日期解析；
    大QMT RPC 柜台行的委托时间还可能以紧凑日期时间（YYYYMMDDHHMMSS）、
    HHMMSS 小整数（如 93001）或当日秒数出现，按数量级归一（当日委托，
    日期一律取今天）。同时兼容已经是字符串/datetime 的情形。
    无法解析时返回 None。
    """
    timestamp = parse_order_time(value)
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return str(value).strip() or None if isinstance(value, str) and not value.strip().isdigit() else None


def is_pending(status):
    """委托是否处于在途（已报未成交/部成）状态。"""
    return status in ACTIVE_ORDER_STATUS


def status_desc(status, fallback=''):
    """委托状态描述，优先使用 QMT 回传的 status_msg。"""
    return fallback or ORDER_STATUS_DESC.get(status, '未知')


def sort_orders(orders):
    """就地排序：在途委托整体在前，组内按时间倒序（缺时间排最后）。"""
    orders.sort(key=lambda o: o.get('order_time') or '', reverse=True)
    orders.sort(key=lambda o: not o.get('is_pending'))
    return orders

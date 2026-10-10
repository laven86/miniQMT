"""Big QMT position and asset adapters."""

from ..code_utils import normalize_stock_code
from ..models import AssetSnapshot, PositionSnapshot


def _attr(obj, names, default=None):
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
    return default


# Leading two-digit prefixes of real exchange-listed 6-digit instruments.
# Broker terminals also report synthetic holdings whose codes are pure digits
# and therefore pass normalize_stock_code (e.g. 888880 aggregated reverse-repo
# / cash-management products), yet they are not exchange instruments: no
# quotes, no history. _full_code drops them so the position table and the
# downstream 10s quote polling never see them.
_EXCHANGE_CODE_PREFIXES = (
    "00", "30",                    # SZ main board / ChiNext
    "60", "68",                    # SH main board / STAR
    "43", "83", "87", "92",        # BSE / NEEQ
    "90", "20",                    # B-shares; 20 also covers 204xxx SH reverse repo
    "40",                          # NEEQ two-board (400xxx)
    "50", "51", "56", "58",        # SH funds / ETF
    "15", "16", "18",              # SZ funds / ETF
    "11", "12", "13",              # CBs: 11x SH / 12x SZ; 1318xx SZ reverse repo
)


def _full_code(instrument_id, exchange_id):
    # Return None for non-stock instruments reported by the broker terminal:
    # 1) letter-coded products (e.g. pledge-style repo RXL001.SZ) rejected by
    #    normalize_stock_code;
    # 2) synthetic pure-digit codes outside the real exchange prefixes above
    #    (e.g. 888880 aggregated reverse repo), which pass normalization but
    #    are not tradable securities.
    # They have no quotes and must not leak into the position table as fake
    # stocks. Callers skip rows whose code is None. Order/quote paths keep
    # using normalize_stock_code directly so invalid codes still fail loudly
    # there.
    code = str(instrument_id or "").strip().upper()
    market = str(exchange_id or "").strip().upper()
    try:
        if "." in code:
            full = normalize_stock_code(code)
        elif market in ("SH", "SZ"):
            full = normalize_stock_code("%s.%s" % (code, market))
        else:
            full = normalize_stock_code(code)
    except ValueError:
        return None
    pure = full.split(".")[0]
    if pure[:2] not in _EXCHANGE_CODE_PREFIXES:
        return None
    return full


class BigQmtPositionProvider:
    def __init__(self, get_trade_detail_data_func, account_type="STOCK"):
        self.get_trade_detail_data = get_trade_detail_data_func
        self.account_type = account_type

    def _require_query_func(self):
        if self.get_trade_detail_data is None:
            raise RuntimeError("get_trade_detail_data is not available in Big QMT runtime")
        return self.get_trade_detail_data

    def get_positions(self, account_id):
        query = self._require_query_func()
        # QMT's get_trade_detail_data can raise on POSITION queries in some
        # states (e.g. context not bound). Degrade to empty like get_asset does.
        try:
            rows = query(account_id, self.account_type, "POSITION") or []
        except Exception:
            return {}
        positions = {}
        for row in rows:
            code = _full_code(
                _attr(row, ("m_strInstrumentID", "instrument_id", "stock_code")),
                _attr(row, ("m_strExchangeID", "exchange_id", "market")),
            )
            if not code:
                # Non-stock instrument (repo/cash product) — skip silently so
                # it never appears in the position table as a fake stock.
                continue
            positions[code] = PositionSnapshot(
                stock_code=code,
                volume=int(_attr(row, ("m_nVolume", "volume"), 0) or 0),
                available=int(_attr(row, ("m_nCanUseVolume", "available", "can_use_volume"), 0) or 0),
                cost=float(_attr(row, ("m_dOpenPrice", "m_dCostPrice", "cost"), 0.0) or 0.0),
                stock_name=str(_attr(row, ("m_strInstrumentName", "stock_name"), "") or ""),
            )
        return positions

    def get_asset(self, account_id):
        query = self._require_query_func()
        rows = []
        for detail_type in ("ACCOUNT", "ASSET"):
            try:
                rows = query(account_id, self.account_type, detail_type) or []
                if rows:
                    break
            except Exception:
                rows = []
        if not rows:
            return AssetSnapshot(account_id=account_id, cash=None, total_asset=None)

        row = rows[0]
        cash = _attr(row, ("m_dAvailable", "m_dAvailableCash", "available_cash", "cash"))
        total_asset = _attr(row, ("m_dBalance", "m_dAsset", "total_asset", "asset"))
        return AssetSnapshot(
            account_id=account_id,
            cash=float(cash) if cash is not None else None,
            total_asset=float(total_asset) if total_asset is not None else None,
        )

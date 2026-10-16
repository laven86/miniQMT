"""Minimal durable order association and pending SIM orders for Chan strategies.

No automatic submit retries. Persist before crossing the broker boundary and
keep ambiguous submissions unknown until a matching broker remark is observed.
"""
import json
import math
import re
import sqlite3
import threading
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ACTIVE = {48, 49, 50, 51, 52, 55}


def rpc_position_rows(raw):
    # BigQMT returns a stock-code keyed mapping; some transports return a list.
    rows = list(raw.values()) if isinstance(raw, dict) else raw
    if not isinstance(rows, list) or any(
        not isinstance(p, dict) or not p.get('stock_code') or p.get('volume') is None
        or p.get('available', p.get('can_use_volume')) is None for p in rows
    ):
        raise RuntimeError('RPC持仓响应缺少证券代码、持仓量或可用量')
    return rows


def quantity_rules(symbol):
    code, market = symbol.split('.')
    if market == 'SH' and code.startswith(('688', '689')):
        return dict(buy_min=200, sell_min=200, buy_step=1, sell_step=1,
                    buy_max=100000, sell_max=50000, tick_size=.01)
    if market == 'SZ' and code.startswith(('300', '301')):
        return dict(buy_min=100, sell_min=100, buy_step=100, sell_step=100,
                    buy_max=300000, sell_max=150000, tick_size=.01)
    if ((market == 'SH' and code.startswith(('600', '601', '603', '605')))
            or (market == 'SZ' and code.startswith(('000', '001', '002', '003')))):
        return dict(buy_min=100, sell_min=100, buy_step=100, sell_step=100,
                    buy_max=1000000, sell_max=1000000, tick_size=.01)
    if market == 'BJ' and code.startswith(('43', '83', '87', '92')):
        return dict(buy_min=100, sell_min=100, buy_step=1, sell_step=1,
                    buy_max=1000000, sell_max=1000000, tick_size=.01)
    raise ValueError('首版仅支持已配置申报规则的A股股票，其他证券暂不可启用')


def check_quantity(qty, rule, side, available=0):
    if isinstance(qty, bool) or not isinstance(qty, int) or qty <= 0 or qty > rule[side + '_max']:
        raise ValueError('委托数量超出合法范围')
    minimum, step = rule[side + '_min'], rule[side + '_step']
    if side == 'sell' and qty == available and (qty < minimum or qty % step):
        return
    if qty < minimum or (qty - minimum) % step:
        raise ValueError('委托数量不符合该板块最小数量和递增单位')


def order_row(order):
    strategy = str(getattr(order, 'strategy_name', '') or '')
    remark = str(getattr(order, 'order_remark', '') or '')
    match = re.search(r'chan:([a-f0-9]{32})', strategy + ' ' + remark)
    raw_status = getattr(order, 'order_status', None)
    return {'order_id': getattr(order, 'order_id', None),
            'stock_code': getattr(order, 'stock_code', ''),
            'trade_type': 'BUY' if getattr(order, 'order_type', 0) == 23 else 'SELL',
            'price': getattr(order, 'price', None),
            'traded_price': getattr(order, 'traded_price', None),
            'volume': int(getattr(order, 'order_volume', 0) or 0),
            'traded_volume': int(getattr(order, 'traded_volume', 0) or 0),
            'status': int(raw_status) if raw_status is not None else -1,
            'strategy': strategy, 'client_order_id': match[1] if match else None,
            'order_time': getattr(order, 'order_time', None)}


class ChanOrderGateway:
    def __init__(self, db_path, *, context, snapshot, submit, cancel, rules,
                 quote, seed_simulation, allowed, clock=time.time, fee_buffer=.002):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.context, self.snapshot_provider, self.submit_provider = context, snapshot, submit
        self.cancel_provider, self.rules_provider, self.quote = cancel, rules, quote
        self.seed_simulation, self.allowed, self.clock = seed_simulation, allowed, clock
        self.fee_buffer = fee_buffer
        self.lock = threading.RLock()
        with self._db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS chan_orders(
                id TEXT PRIMARY KEY, context TEXT NOT NULL, fingerprint TEXT NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS chan_sim_accounts(context TEXT PRIMARY KEY, state TEXT NOT NULL);''')

    def _db(self):
        return sqlite3.connect(self.path, timeout=10)

    def _key(self):
        return json.dumps(self.context(), sort_keys=True)

    def _orders(self, db, key):
        return [json.loads(row[0]) for row in db.execute('SELECT state FROM chan_orders WHERE context=? ORDER BY rowid', (key,))]

    @staticmethod
    def _save(db, order):
        db.execute('UPDATE chan_orders SET state=? WHERE id=?', (json.dumps(order), order['client_order_id']))

    def _sim(self, db, key):
        row = db.execute('SELECT state FROM chan_sim_accounts WHERE context=?', (key,)).fetchone()
        if row:
            return json.loads(row[0])
        state = self.seed_simulation()
        state.setdefault('unsettled', [])
        db.execute('INSERT INTO chan_sim_accounts VALUES(?,?)', (key, json.dumps(state)))
        return state

    def state(self, symbols=()):
        with self.lock, self._db() as db:
            ctx, key = self.context(), self._key()
            own = self._orders(db, key)
            if ctx['mode'] == 'simulation':
                state = self._sim(db, key)
                today = datetime.fromtimestamp(self.clock()).date().isoformat()
                for fill in state['unsettled'][:]:
                    if fill['day'] < today:
                        state['positions'][fill['symbol']]['available'] += fill['qty']
                        state['unsettled'].remove(fill)
                for order in own:
                    if order['status'] not in ACTIVE:
                        continue
                    if datetime.fromtimestamp(order['created_at']).date().isoformat() < today:
                        order['status'] = 53 if order['traded_volume'] else 54
                        self._save(db, order)
                        continue
                    tick = self.quote(order['stock_code']) or {}
                    price, ts = tick.get('lastPrice'), tick.get('time')
                    if not price or not ts or not math.isfinite(price) or price <= 0:
                        continue
                    ts = ts / 1000 if ts > 1e11 else ts
                    if not 0 <= self.clock() - ts <= 5:
                        continue
                    buy = order['trade_type'] == 'BUY'
                    if buy and price > order['price']:
                        continue
                    if not buy and order.get('price') and price < order['price']:
                        order['status'] = 54
                        self._save(db, order)
                        continue
                    position = state['positions'].setdefault(order['stock_code'], {'volume': 0, 'available': 0})
                    qty = order['volume'] - order['traded_volume']
                    if not buy:
                        qty = min(qty, position['available'])
                    if not qty:
                        continue
                    fill_price = min(price, order['price']) if buy else price
                    cost = qty * fill_price
                    state['cash'] += -cost * (1 + self.fee_buffer) if buy else cost * (1 - self.fee_buffer)
                    position['volume'] += qty if buy else -qty
                    if buy:
                        state['unsettled'].append({'symbol': order['stock_code'], 'qty': qty, 'day': today})
                    else:
                        position['available'] -= qty
                    order['traded_volume'] += qty
                    order['traded_price'] = fill_price
                    order['status'] = 56 if order['traded_volume'] == order['volume'] else 55
                    self._save(db, order)
                db.execute('UPDATE chan_sim_accounts SET state=? WHERE context=?', (json.dumps(state), key))
                buy_frozen = sum(o['price'] * (o['volume'] - o['traded_volume']) * (1 + self.fee_buffer)
                                 for o in own if o['trade_type'] == 'BUY' and o['status'] in ACTIVE)
                sell_frozen = {}
                for o in own:
                    if o['trade_type'] == 'SELL' and o['status'] in ACTIVE:
                        sell_frozen[o['stock_code']] = sell_frozen.get(o['stock_code'], 0) + o['volume'] - o['traded_volume']
                positions = [{'symbol': symbol, 'volume': p['volume'],
                              'available': max(0, p['available'] - sell_frozen.get(symbol, 0))}
                             for symbol, p in state['positions'].items() if p['volume']]
                return {**ctx, 'cash': state['cash'] - buy_frozen, 'positions': positions, 'orders': own}
            snapshot = self.snapshot_provider()
            # A durable client tag survives RPC id remapping and process restarts.
            for order in own:
                matches = [r for r in snapshot['orders'] if r.get('client_order_id') == order['client_order_id']]
                if len(matches) == 1:
                    matches[0]['accepted_at'] = order['created_at']
                    order.update(matches[0])
                    order['submit_status'] = 'success'
                    self._save(db, order)
                elif not matches and order['status'] in (53, 54, 56, 57):
                    snapshot['orders'].append(order)
            return {**ctx, **snapshot}

    @staticmethod
    def response(order):
        return {'status': order.get('submit_status', 'unknown'), 'order_id': order.get('order_id'),
                'client_order_id': order['client_order_id'], 'message': order.get('message', ''),
                'volume': order['volume'], 'traded_volume': order['traded_volume'], 'order_status': order['status']}

    def submit(self, payload, side):
        client_id = payload.get('client_order_id', '')
        if not re.fullmatch('[a-f0-9]{32}', client_id):
            raise ValueError('client_order_id须为32位十六进制ID')
        fingerprint = json.dumps({'side': side, **payload}, sort_keys=True)
        with self.lock:
            key = self._key()
            ctx = self.context()
            expected = (payload.get('account_id'), payload.get('trading_mode'), payload.get('channel'))
            if any(v is not None for v in expected) and expected != (ctx['account_id'], ctx['mode'], ctx['channel']):
                raise ValueError('请求账户、交易模式或通道与当前配置不一致')
            with self._db() as db:
                previous = db.execute('SELECT context,fingerprint,state FROM chan_orders WHERE id=?', (client_id,)).fetchone()
                if previous:
                    if previous[:2] != (key, fingerprint):
                        raise ValueError('提交ID不能复用于不同参数或账户模式')
                    return self.response(json.loads(previous[2]))
            self.allowed(side)
            state = self.state([payload['stock_code']])
            with self._db() as db:
                if any(o['stock_code'] == payload['stock_code'] and o.get('submit_status') == 'unknown'
                       for o in self._orders(db, key)):
                    raise ValueError('该标的存在提交结果未知的委托，请先对账')
            rule = self.rules_provider(payload['stock_code'])
            qty = payload.get('volume')
            position = next((p for p in state['positions'] if p['symbol'] == payload['stock_code']), {'available': 0})
            check_quantity(qty, rule, side, position['available'])
            if side == 'sell' and qty > position['available']:
                raise ValueError('卖出数量超过柜台可用数量')
            price, kind = payload.get('price'), payload.get('price_type')
            if side == 'buy':
                if kind != 11 or not price or not math.isfinite(price) or price <= 0:
                    raise ValueError('策略买入须使用正数显式限价')
                if Decimal(str(price)) % Decimal(str(rule['tick_size'])):
                    raise ValueError('限价不符合价格档位')
                if state['mode'] == 'live' and rule.get('requires_live_price_check'):
                    if not rule.get('upper_limit') or not rule.get('lower_limit'):
                        raise ValueError('当日合法价格边界缺失，暂不支持该证券')
                    tick = self.quote(payload['stock_code']) or {}
                    ts = tick.get('time') or 0
                    ts = ts / 1000 if ts > 1e11 else ts
                    asks = tick.get('askPrice') or []
                    ask = asks[0] if asks else 0
                    if not 0 <= self.clock() - ts <= 5 or not ask or not math.isfinite(ask) or ask <= 0:
                        raise ValueError('缺少有效实时卖一报价，不能完成限价预检')
                    # Conservative subset of the continuous-auction price cage;
                    # do not widen the user's frozen limit to make it acceptable.
                    if price > ask * 1.02 + 1e-9:
                        raise ValueError('买入限价高于当前卖一价2%，等待下一次有效触发')
                for bound, sign in (('upper_limit', 1), ('lower_limit', -1)):
                    if rule.get(bound) and (price - rule[bound]) * sign > 1e-9:
                        raise ValueError('限价超出证券当日合法范围')
                if price * qty * (1 + self.fee_buffer) > state['cash']:
                    raise ValueError('含费可用资金不足')
            elif kind != 5 or price != 0:
                raise ValueError('策略卖出须使用最新价类型5，price须为0')
            order = {'client_order_id': client_id, 'stock_code': payload['stock_code'],
                     'trade_type': side.upper(), 'price': price, 'price_type': kind,
                     'volume': qty, 'traded_volume': 0, 'traded_price': None,
                     'status': -1, 'submit_status': 'unknown', 'order_id': None,
                     'strategy': payload.get('strategy_id', ''), 'intent_id': payload.get('intent_id'),
                     'created_at': self.clock(), 'accepted_at': self.clock(), 'order_time': self.clock()}
            with self._db() as db:
                db.execute('INSERT INTO chan_orders VALUES(?,?,?,?)', (client_id, key, fingerprint, json.dumps(order)))
            # Persist intent before the irreversible network call; no retry here.
            if self._key() != key:
                order.update(submit_status='failed', status=57, message='交易上下文已改变，未发单')
            elif state['mode'] == 'simulation':
                order.update(order_id='SIM_' + client_id, status=50, submit_status='success')
            else:
                try:
                    order_id = self.submit_provider(payload, side)
                    if not isinstance(order_id, bool) and order_id is not None and str(order_id) not in ('', '0', '-1'):
                        order.update(order_id=order_id, status=50, submit_status='success')
                    else:
                        order['message'] = '柜台提交结果未知，请通过备注关联查询，禁止重发'
                except Exception as exc:
                    order['message'] = f'柜台提交结果未知: {exc}'
            with self._db() as db:
                self._save(db, order)
            return self.response(order)

    def cancel(self, order_id):
        with self.lock, self._db() as db:
            key, ctx = self._key(), self.context()
            order = next((o for o in self._orders(db, key) if str(o.get('order_id')) == str(order_id)), None)
            if not order:
                return None
            if order['status'] not in ACTIVE:
                return {'status': 'failed', 'message': '订单已终结'}
            if ctx['mode'] == 'simulation':
                order['status'] = 53 if order['traded_volume'] else 54
                self._save(db, order)
                return {'status': 'success', 'message': '模拟撤单完成'}
            try:
                result = self.cancel_provider(order_id)
                return {'status': 'success' if result == 0 else 'failed', 'message': '撤单申请已发送' if result == 0 else '柜台拒绝撤单'}
            except Exception as exc:
                return {'status': 'unknown', 'message': f'撤单结果未知: {exc}'}


def build_gateway(config, position_manager, data_manager):
    def context():
        trader = getattr(position_manager(), 'qmt_trader', None)
        simulation = bool(config.ENABLE_SIMULATION_MODE)
        account = (config.ACCOUNT_CONFIG.get('account_id') if simulation else getattr(trader, 'account', None)) or config.ACCOUNT_CONFIG.get('account_id')
        if not account and not simulation:
            raise ValueError('账户不可确认')
        return {'account_id': str(account or 'SIMULATION'),
                'mode': 'simulation' if simulation else 'live',
                'channel': 'SIM' if simulation else type(trader).__name__}

    def connection():
        trader = getattr(position_manager(), 'qmt_trader', None)
        if trader is None:
            raise RuntimeError('交易通道未连接')
        if hasattr(trader, '_bq'):
            if not getattr(trader, '_allow_order', False):
                raise ValueError('RPC交易通道处于只读模式')
            return trader._bq, trader._bq_acc, True
        api = getattr(trader, 'xt_trader', None)
        if api is None or not hasattr(api, 'query_stock_asset') or not hasattr(api, 'order_stock'):
            raise ValueError('该通道暂不支持可靠的同步发单和柜台快照')
        return api, trader.acc, False

    def snapshot():
        api, account, rpc = connection()
        if rpc:
            # The compatibility API falls back to cached positions on errors;
            # executable strategies require a successful fresh broker query.
            from types import SimpleNamespace
            account_id = context()['account_id']
            raw_asset = api.client.call('query_stock_asset', {'account_id': account_id}, account_id=account_id)
            if not isinstance(raw_asset, dict) or raw_asset.get('cash') is None:
                raise RuntimeError('RPC资金快照不可确认')
            asset = SimpleNamespace(cash=raw_asset['cash'])
            raw = api.client.call('query_stock_positions', {'account_id': account_id}, account_id=account_id)
            positions = [SimpleNamespace(stock_code=p['stock_code'], volume=p['volume'],
                         can_use_volume=p.get('available', p.get('can_use_volume'))) for p in rpc_position_rows(raw)]
        else:
            asset = api.query_stock_asset(account)
            positions = api.query_stock_positions(account)
        orders = api.query_stock_orders(account, cancelable_only=False, **({'strategy_name': ''} if rpc else {}))
        if asset is None or positions is None or orders is None:
            raise RuntimeError('柜台快照不完整，禁止策略执行')
        cash = getattr(asset, 'cash', getattr(asset, 'available_cash', None))
        if cash is None or not math.isfinite(float(cash)):
            raise RuntimeError('柜台可用资金不可确认')
        result = []
        for p in positions:
            available = getattr(p, 'can_use_volume', getattr(p, 'available_amount', None))
            if available is None:
                raise RuntimeError('柜台可卖数量不可确认')
            result.append({'symbol': p.stock_code, 'volume': int(p.volume), 'available': int(available)})
        return {'cash': float(cash), 'positions': result, 'orders': [order_row(o) for o in orders]}

    def quote(symbol):
        return (data_manager().xt.get_full_tick([symbol]) or {}).get(symbol, {})

    def rules(symbol):
        rule = quantity_rules(symbol)
        rule.update(requires_live_price_check=True)
        try:
            detail = data_manager().xt.get_instrument_detail(symbol) or {}
            rule['lower_limit'] = detail.get('DownStopPrice')
            rule['upper_limit'] = detail.get('UpStopPrice')
        except Exception:
            pass
        return rule

    def submit(payload, side):
        api, account, _ = connection()
        # Chan has independent fixed portions; the legacy dynamic-signal gate
        # rejects any existing order, including legitimate other Chan portions.
        # Quantity, availability, permission and durable duplicate checks run above.
        tag = 'chan:' + payload['client_order_id']
        return api.order_stock(account, payload['stock_code'], 23 if side == 'buy' else 24,
                               payload['volume'], payload['price_type'], payload.get('price') or 0, tag, tag)

    def cancel(order_id):
        api, account, rpc = connection()
        result = api.cancel_order_stock(account, order_id)
        return (0 if result is True else -1) if rpc else result

    def allowed(side):
        if not getattr(config, 'ENABLE_ALLOW_' + side.upper()):
            raise ValueError('该侧交易权限未开启')
        if not config.ENABLE_SIMULATION_MODE and not config.is_trade_time():
            raise ValueError('当前不在允许下单的交易时段')
        if not config.ENABLE_SIMULATION_MODE:
            now = datetime.now()
            minute = now.hour * 60 + now.minute
            if now.weekday() >= 5 or not (570 <= minute < 690 or 780 <= minute < 897):
                raise ValueError('策略首版仅在连续竞价时段提交委托')

    def seed():
        frame = position_manager().get_all_positions()
        return {'cash': float(config.SIMULATION_BALANCE), 'positions': {
            str(p['stock_code']): {'volume': int(p['volume']), 'available': int(p['available'])}
            for p in frame.to_dict('records')}}

    return ChanOrderGateway(config.CHAN_ORDER_DB_PATH, context=context, snapshot=snapshot,
        submit=submit, cancel=cancel, rules=rules, quote=quote, seed_simulation=seed,
        allowed=allowed, fee_buffer=config.CHAN_ORDER_FEE_BUFFER)

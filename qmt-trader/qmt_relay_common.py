# -*- coding: utf-8 -*-
"""大QMT 桥公共模块：命令白名单、请求校验、执行分发与值编解码、Redis 客户端构造。

历史：这些逻辑原属 TCP 中继 qmt_redis_relay.py（监听 127.0.0.1:51761）。
2026-10-23 券商升级后，大QMT 主客户端周期枚举内嵌策略进程的 TCP 连接表，
任何对端不在券商白名单——包括 127.0.0.1 回环——都会触发 onIllegalIPFind
「不可用IP」并强制停止所有策略。TCP 中继方案证伪并删除，共享的校验、
执行与编解码逻辑抽到本模块，供文件桥守护进程 qmt_file_relay.py 复用。

安全面（与原 TCP 中继完全一致，不做第二份实现）：
    - 命令白名单：只放行大QMT 策略实际用到的命令（见 bigqmt_signal_trader
      各模块），拒绝 FLUSHDB/CONFIG/EVAL/KEYS 等；
    - key 前缀校验：所有 key 必须位于允许的前缀下（默认 bigqmt:）；
    - 共享 token 鉴权：token 非空时请求必须携带一致 token，空则不校验。

值编码（JSON 帧内保持 redis-py 类型语义）：
    bytes  -> {"__b64__": base64}
    dict   -> {"__map__": [[k, v], ...]}（JSON 对象键只能是字符串，
              hgetall 返回的 bytes 键用键值对列表保真）
"""

import base64

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

# 大QMT 策略实际用到的命令（见 bigqmt_signal_trader 各模块）。
# 每个命令声明哪些位置参数是 key，用于前缀校验。
# key_args: 参数下标列表；"all" 表示全部位置参数都是 key（delete）。
ALLOWED_COMMANDS = {
    "get": {"key_args": [0]},
    "set": {"key_args": [0]},
    "setnx_ex": {"key_args": [0]},
    "setex": {"key_args": [0]},
    "delete": {"key_args": "all"},
    "expire": {"key_args": [0]},
    "rpush": {"key_args": [0]},
    "lpop": {"key_args": [0]},
    "publish": {"key_args": [0]},
    "hset": {"key_args": [0]},
    "hgetall": {"key_args": [0]},
    "xadd": {"key_args": [0]},
    "ping": {"key_args": []},
}


class RelayError(Exception):
    """请求被拒绝或执行失败（错误信息会原样返回给客户端）。"""


def _b64(value):
    return {"__b64__": base64.b64encode(value).decode("ascii")}


def encode_value(value):
    """把 Redis 返回值转成可 JSON 传输的结构，bytes 用 __b64__ 包裹。"""
    if isinstance(value, bytes):
        return _b64(value)
    if isinstance(value, (list, tuple)):
        return [encode_value(item) for item in value]
    if isinstance(value, dict):
        # JSON 对象的键只能是字符串；hgetall 返回 bytes 键，用键值对列表保真。
        return {"__map__": [[encode_value(key), encode_value(item)] for key, item in value.items()]}
    if isinstance(value, bool) or value is None or isinstance(value, (int, float, str)):
        return value
    return str(value)


def decode_value(value):
    """还原客户端发来的参数：__b64__ 包裹的还原成 bytes。"""
    if isinstance(value, dict):
        if set(value.keys()) == {"__b64__"} and isinstance(value["__b64__"], str):
            try:
                return base64.b64decode(value["__b64__"].encode("ascii"))
            except Exception as exc:
                raise RelayError("bad base64 argument: %s" % exc)
        return {decode_value(key): decode_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decode_value(item) for item in value]
    return value


def _keys_of(spec, args):
    """按命令声明从参数里取出需要校验前缀的 key。"""
    key_args = spec["key_args"]
    indexes = range(len(args)) if key_args == "all" else list(key_args)
    keys = []
    for index in indexes:
        if index >= len(args):
            continue
        value = args[index]
        if isinstance(value, bytes):
            value = value.decode("utf-8", "replace")
        keys.append(str(value))
    return keys


def validate_request(request, token, key_prefix):
    """校验请求结构、token、命令白名单与 key 前缀，返回 (cmd, args)。"""
    if not isinstance(request, dict):
        raise RelayError("request must be a json object")
    if token and str(request.get("token") or "") != token:
        raise RelayError("bad token")
    cmd = str(request.get("cmd") or "").strip().lower()
    spec = ALLOWED_COMMANDS.get(cmd)
    if spec is None:
        raise RelayError("command not allowed: %s" % cmd)
    args = decode_value(request.get("args") or [])
    if not isinstance(args, list):
        raise RelayError("args must be a list")
    prefix = str(key_prefix or "")
    if prefix:
        for key in _keys_of(spec, args):
            if not key.startswith(prefix):
                raise RelayError("key outside allowed prefix %r: %s" % (prefix, key))
    return cmd, args


def execute(client, cmd, args):
    """在真实 Redis 上执行命令。hset 兼容 mapping 调用，setnx_ex 对应 SET NX EX。"""
    if cmd == "hset" and len(args) == 2 and isinstance(args[1], dict):
        return client.hset(args[0], mapping=args[1])
    if cmd == "setnx_ex":
        return client.set(args[0], args[1], nx=True, ex=int(args[2]))
    if cmd == "setex":
        return client.set(args[0], args[2], ex=int(args[1]))
    return getattr(client, cmd)(*args)


def build_redis_client(host, port, db, password):
    # 关闭 redis-py 8 默认的 10 次退避重试：Redis 卡住时单条命令会被放大到
    # 约 1 分钟，桥被拖住，大QMT 侧所有请求一起超时。失败直接回错给客户端。
    return redis.Redis(
        host=host,
        port=int(port),
        db=int(db),
        password=password or None,
        socket_connect_timeout=2.0,
        socket_timeout=5.0,
        health_check_interval=30,
        retry=Retry(NoBackoff(), 0),
    )

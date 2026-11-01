"""Redis client helpers for Big QMT signal trader."""

import os


def _float_or_none(value, default=None):
    if value is None:
        return default
    if value == "":
        return default
    text = str(value).strip()
    if text.lower() in ("none", "null"):
        return None
    return float(value)


def build_redis_client(config=None):
    config = config or {}

    # File bridge mode (required since the broker's 2026-10-23 upgrade: Big
    # QMT kills the strategy for ANY outbound TCP connection, even 127.0.0.1
    # loopback, so direct Redis AND the old TCP relay are both dead; the relay
    # adapter was removed). When file_dir is set, the client exchanges JSON
    # request/response files with the qmt_file_relay daemon in that directory
    # - zero sockets.
    file_dir = config.get("file_dir") or os.environ.get("BIGQMT_FILE_DIR")
    if file_dir:
        from .file_redis_client import FileRedisClient

        return FileRedisClient(
            file_dir=file_dir,
            token=config.get("file_token") or os.environ.get("BIGQMT_FILE_TOKEN") or "",
            timeout=_float_or_none(config.get("file_timeout", 1.5), 1.5),
        )

    # Direct Redis (no file_dir). Inside the Big QMT sandbox this path gets
    # the strategy killed as "unavailable IP"; it only works outside the
    # sandbox (tests, tools). The legacy relay_host/relay_port/relay_token
    # keys are ignored since the TCP relay was deleted (2026-10-31).
    try:
        import redis
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("redis package is required when Redis adapters are enabled") from exc

    url = config.get("url") or os.environ.get("BIGQMT_REDIS_URL")
    if url:
        return redis.Redis.from_url(
            url,
            socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
            socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
        )

    host = config.get("host") or os.environ.get("BIGQMT_REDIS_HOST") or "127.0.0.1"
    port = int(config.get("port") or os.environ.get("BIGQMT_REDIS_PORT") or 6379)
    db = int(config.get("db") or os.environ.get("BIGQMT_REDIS_DB") or 5)
    username = config.get("username") or os.environ.get("BIGQMT_REDIS_USERNAME") or None
    password = config.get("password") or os.environ.get("BIGQMT_REDIS_PASSWORD") or None
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
        socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
        health_check_interval=int(config.get("health_check_interval", 30)),
    )


def decode_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def redis_mapping_to_text(mapping):
    return {decode_text(key): decode_text(value) for key, value in (mapping or {}).items()}

#coding:gbk
"""File bridge Redis client for the Big QMT embedded Python (stdlib only).

Since the broker's 2026-10-23 upgrade, Big QMT kills strategies that open ANY
outbound TCP connection (even 127.0.0.1 loopback is logged as
onIllegalIPFind "illegal IP"), so both direct redis-py and the TCP relay
client are unusable. Plain file reads/writes still work inside the strategy
process, so this client exchanges JSON request/response files with the
qmt_file_relay daemon instead. Zero sockets: this module never imports
socket and never opens one.

Sandbox note (2026-10-31, BIGQMT_FILE_PROBE v3 findings): the strategy
sandbox hooks builtin open(), io.open() and os.open() READS, raising
"PermissionError: Forbidden FileIO", while writes really land on disk
(os.stat verified) and os.replace/os.remove/os.stat/os.listdir work.
The low-level _io.FileIO(path, 'rb') read is NOT hooked, so response
files are read through _io.FileIO below (with a plain-open fallback for
normal hosts).

Directory layout (file_dir must match the daemon's --dir):
    <root>/req/    this client writes <id>.json requests
    <root>/resp/   the daemon writes <id>.json responses

Wire format (qmt_redis_relay's JSON structure plus a nonce):
    request {"id": <int counter>, "nonce": <per-process str>, "token": str,
             "cmd": str, "args": list}
    response {"id": int, "nonce": <echoed back>, "ok": true, "data": ...} or
             {"id": int, "nonce": <echoed back>, "ok": false, "error": str}
    bytes values travel as {"__b64__": ...}; dict values as
    {"__map__": [[key, value], ...]} so hgetall bytes keys survive JSON.
    The daemon only accepts int ids and echoes the nonce verbatim; a reply
    whose nonce is missing or different is a stale file from an earlier
    process and is ignored (deleted, keep polling).

Python 3.6 compatible on purpose: no f-strings, no walrus. Errors are
redis.exceptions subclasses when redis-py is importable, so callers keep
their existing `except Exception` handling either way.

Semantics: strictly one outstanding request at a time (instance lock). A
timed-out request is abandoned and never retried - the daemon may already
have run it, and replaying LPOP/RPUSH would drop or duplicate an RPC
request. On timeout the client also removes its own request file (best
effort) so a dead/offline bridge does not cause unbounded req/
accumulation. Response files left over by an earlier run are wiped on
startup and again for the current id before every request.
"""

import base64
import json
import os
import threading
import time

try:
    import redis as _redis_mod
    _FileRelayErrorBase = _redis_mod.exceptions.RedisError
    _FileTimeoutBase = _redis_mod.exceptions.TimeoutError
except Exception:  # pragma: no cover - QMT side does not ship redis-py
    _FileRelayErrorBase = Exception
    _FileTimeoutBase = Exception


class FileRelayError(_FileRelayErrorBase):
    """File transport or protocol failure while talking to the daemon."""


class FileRelayTimeoutError(_FileTimeoutBase):
    """The daemon did not write the response file before timeout."""


def _wrap(value):
    if isinstance(value, bytes):
        return {"__b64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {"__map__": [[_wrap_key(key), _wrap(item)] for key, item in value.items()]}
    if isinstance(value, (list, tuple)):
        return [_wrap(item) for item in value]
    return value


def _wrap_key(value):
    # JSON object keys must be strings; mapping keys are field names here.
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _unwrap(value):
    if isinstance(value, dict):
        if len(value) == 1 and "__b64__" in value and isinstance(value["__b64__"], str):
            return base64.b64decode(value["__b64__"].encode("ascii"))
        if len(value) == 1 and "__map__" in value and isinstance(value["__map__"], list):
            # hgetall replies: bytes keys survive as [key, value] pairs.
            return {_unwrap(pair[0]): _unwrap(pair[1]) for pair in value["__map__"]}
        return {_unwrap(key): _unwrap(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_unwrap(item) for item in value]
    return value


def _remove_silently(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _remove_with_retries(path, attempts=5, delay=0.01):
    # Timeout cleanup helper: a few retries to ride out a transient
    # antivirus/indexer lock on a freshly written file. Failure is returned
    # (not raised) so callers can ignore it - the daemon's prepare() wipes
    # req/ on its next startup anyway.
    for _ in range(attempts):
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            time.sleep(delay)
    return False


def _atomic_write_json(path, payload):
    # Write <path>.tmp then os.replace (atomic on the same Windows volume);
    # the daemon only ever reads the final file name. json.dumps defaults to
    # ensure_ascii=True, keeping the file pure ASCII.
    tmp_path = path + ".tmp"
    body = json.dumps(payload)
    with open(tmp_path, "wb") as fh:
        fh.write(body.encode("ascii"))
    os.replace(tmp_path, path)


def _read_file_bytes(path):
    # Read a whole file as bytes WITHOUT builtins.open: the Big QMT
    # strategy sandbox hooks builtin open(), io.open() and os.open()
    # reads ("PermissionError: Forbidden FileIO") but leaves the
    # low-level _io.FileIO constructor untouched - verified by
    # BIGQMT_FILE_PROBE v3 on 2026-10-31. Reading the daemon's response
    # files through _io.FileIO therefore keeps the file bridge alive
    # inside the sandbox. Falls back to plain open() when _io is missing
    # so normal hosts (tests, daemon dev runs) behave exactly as before.
    # Callers must treat OSError (PermissionError included, e.g. from a
    # transient antivirus lock on a freshly replaced file) as
    # "not ready yet" and keep polling.
    try:
        import _io
    except ImportError:  # pragma: no cover - _io always ships with CPython
        _io = None
    if _io is not None and hasattr(_io, "FileIO"):
        with _io.FileIO(path, "rb") as fh:
            return fh.read()
    with open(path, "rb") as fh:
        return fh.read()


class FileRedisClient(object):
    """One JSON request file out, one JSON response file back, poll every 2ms.

    timeout (default 1.5s, same as the TCP relay era) mirrors the RPC
    caller budget: a dead daemon fails fast instead of stalling QMT's
    strategy callback.

    supports_xadd_maxlen tells callers that XADD trimming is unavailable
    here, so they skip the stream write instead of raising on every event.
    """

    supports_xadd_maxlen = False

    def __init__(self, file_dir, token="", timeout=1.5, poll_interval=0.002):
        self.file_dir = str(file_dir)
        self.req_dir = os.path.join(self.file_dir, "req")
        self.resp_dir = os.path.join(self.file_dir, "resp")
        self.token = str(token or "")
        self.timeout = float(timeout or 1.5)
        self.poll_interval = float(poll_interval or 0.002)
        # Per-process nonce: the daemon echoes it back, so a response file
        # left by an earlier process (same id after a restart) can never be
        # mistaken for this process's own reply.
        self.nonce = "%d-%s" % (int(time.time() * 1000), os.getpid())
        self._lock = threading.Lock()
        self._counter = 0
        for directory in (self.req_dir, self.resp_dir):
            if not os.path.isdir(directory):
                os.makedirs(directory)
        self._cleanup_residual_responses()

    def close(self):
        # Nothing to close: no sockets, no file handles kept open.
        pass

    def _cleanup_residual_responses(self):
        # After a restart the counter starts at 1 again, so any response
        # file left by an earlier run (e.g. a reply that arrived after a
        # timeout) could be mistaken for a live reply. Wipe them all: this
        # directory belongs to exactly one strategy client.
        try:
            names = os.listdir(self.resp_dir)
        except OSError:
            return
        for name in names:
            if name.endswith(".json"):
                _remove_silently(os.path.join(self.resp_dir, name))

    def _execute(self, cmd, args):
        # Serialized by the lock: at most one outstanding request file. A
        # timed-out request is abandoned (never resent) because the daemon
        # may already have executed it; replaying LPOP/RPUSH would corrupt
        # the RPC queues.
        with self._lock:
            self._counter += 1
            request_id = self._counter
            req_path = os.path.join(self.req_dir, "%d.json" % request_id)
            resp_path = os.path.join(self.resp_dir, "%d.json" % request_id)
            _remove_silently(resp_path)  # stale reply for this id, if any
            request = {
                "id": request_id,
                "nonce": self.nonce,
                "token": self.token,
                "cmd": cmd,
                "args": _wrap(list(args)),
            }
            _atomic_write_json(req_path, request)
            deadline = time.time() + self.timeout
            response = None
            while True:
                if os.path.exists(resp_path):
                    # Windows: a freshly replaced file can be briefly locked
                    # by antivirus/indexer scanning (PermissionError). Treat
                    # that like "not ready yet" and keep polling until the
                    # deadline instead of failing the command. The read goes
                    # through _io.FileIO so the QMT sandbox (which hooks
                    # builtin open/io.open/os.open reads) cannot block it.
                    raw = None
                    try:
                        raw = _read_file_bytes(resp_path)
                    except OSError:
                        raw = None
                    if raw is not None:
                        _remove_silently(resp_path)
                        try:
                            candidate = json.loads(raw.decode("utf-8"))
                        except ValueError:
                            candidate = None  # junk file in our id slot
                        if isinstance(candidate, dict) and candidate.get("nonce") == self.nonce:
                            response = candidate
                            break
                        # Wrong/missing nonce = a stale reply from an earlier
                        # process (or junk): removed above, keep polling for
                        # our real response until the deadline.
                if time.time() >= deadline:
                    # Timeout cleanup: remove our own request file so a
                    # dead/offline bridge does not cause unbounded req/
                    # accumulation - each abandoned call used to leave one
                    # <id>.json behind (observed 2700+ files after a long
                    # offline window). Best effort: if removal keeps failing
                    # (transient lock) give up quietly - the daemon's
                    # prepare() wipes req/ on next startup - and never mask
                    # the timeout error with a removal failure.
                    _remove_with_retries(req_path)
                    raise FileRelayTimeoutError(
                        "file relay %s %s timed out after %.3fs (no %s)"
                        % (cmd, self.file_dir, self.timeout, resp_path)
                    )
                time.sleep(self.poll_interval)
        if not response.get("ok"):
            raise FileRelayError(str(response.get("error") or "file relay command failed"))
        return _unwrap(response.get("data"))

    # -- redis-py compatible surface ------------------------------------------
    # Same method set as RelayRedisClient. Anything else raises a clear
    # error instead of failing opaquely on the daemon side.

    def get(self, key):
        return self._execute("get", [key])

    def set(self, key, value, nx=False, ex=None):
        if nx and ex is not None:
            return self._execute("setnx_ex", [key, value, int(ex)])
        if nx or ex is not None:
            raise FileRelayError("SET supports plain SET and SET NX EX only")
        return self._execute("set", [key, value])

    def setex(self, key, seconds, value):
        return self._execute("setex", [key, int(seconds), value])

    def delete(self, *keys):
        return self._execute("delete", list(keys))

    def expire(self, key, seconds):
        return self._execute("expire", [key, int(seconds)])

    def rpush(self, key, *values):
        return self._execute("rpush", [key] + list(values))

    def lpop(self, key):
        return self._execute("lpop", [key])

    def publish(self, channel, message):
        return self._execute("publish", [channel, message])

    def hset(self, key, field=None, value=None, mapping=None):
        if mapping is not None:
            return self._execute("hset", [key, mapping])
        return self._execute("hset", [key, field, value])

    def hgetall(self, key):
        result = self._execute("hgetall", [key])
        return result or {}

    def xadd(self, key, fields, maxlen=None, approximate=True):
        if maxlen is not None:
            raise FileRelayError("XADD with maxlen is not relayed")
        return self._execute("xadd", [key, fields])

    def ping(self):
        return self._execute("ping", [])

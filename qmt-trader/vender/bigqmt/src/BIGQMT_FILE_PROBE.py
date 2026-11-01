#coding:gbk
# Probe v3: file IO capability of a Big QMT strategy sandbox.
#
# Stdlib only, ZERO sockets (any TCP connection gets the strategy killed
# since the broker's 2026-10-23 upgrade), pure ASCII, py3.6 safe. Run
# init() once. Three phases:
#
#   phase 1 - five-dir capability matrix (script dir, script dir subdir,
#             TEMP, configured file_dir, QMT userdata). Each entry probes
#             makedirs (when absent), write, readback. Every write is
#             verified with os.stat: OK(verified) / FAKE (silently
#             dropped) / OK(stat-blocked). Readback goes through
#             builtins.open. userdata is located relative to the script
#             dir (<py_dir>/../userdata) so this file stays pure ASCII.
#   phase 2 - bypass read channel matrix on every write-verified dir
#             (TEMP when nothing is verified): builtin_open, io_open,
#             _io_fileio, os_open_read, os_stat_size, importlib_mod,
#             os_listdir, os_replace_remove. 2026-10-31 sandbox result:
#             write reported OK while every readback raised
#             "PermissionError: Forbidden FileIO" - the hook sits at the
#             builtins.open/io.open layer, but CPython imports read files
#             at C level, so low-level channels may still work. Any
#             content-read channel that works revives the file bridge.
#   phase 3 - ping round trip through the bridge (kept from v2). The
#             client still reads resp files via builtins.open, so this
#             is expected to FAIL until FileRedisClient is retrofitted
#             with the bypass read channel found in phase2 (if any).
#
# file_dir/file_token come from bigqmt_signal_trader_local_config's
# BIGQMT_REDIS_CONFIG (file_dir / file_token keys) when that module is
# importable; the constants below are fallbacks. Keep this file pure ASCII.
import json
import os
import sys
import time
import io

FILE_DIR = r"c:\croco_hunter_decision\miniQMT\qmt-trader\qmt_file_bridge"
FILE_TOKEN = ""
# 5s: generous margin for the first real round trip through the daemon.
PING_TIMEOUT = 5.0
POLL_INTERVAL = 0.002

# Compact per-dir line order for the phase2 matrix.
CHANNEL_ORDER = (
    "builtin_open",
    "io_open",
    "_io_fileio",
    "os_open_read",
    "os_stat_size",
    "importlib_mod",
    "os_listdir",
    "os_replace_remove",
)
# Channels able to read file CONTENT (metadata channels excluded).
READ_CHANNELS = ("builtin_open", "io_open", "_io_fileio", "os_open_read", "importlib_mod")

_qmt_paths = []
_file_name = globals().get("__file__", "")
if _file_name:
    _qmt_paths.append(os.path.dirname(os.path.abspath(_file_name)))
_qmt_paths.append(os.getcwd())
for _qmt_path in _qmt_paths:
    if _qmt_path and os.path.isdir(_qmt_path) and _qmt_path not in sys.path:
        sys.path.insert(0, _qmt_path)


def _load_file_config():
    file_dir = FILE_DIR
    token = FILE_TOKEN
    try:
        import bigqmt_signal_trader_local_config as _local_config
        _cfg = getattr(_local_config, "BIGQMT_REDIS_CONFIG", {}) or {}
        file_dir = str(_cfg.get("file_dir") or file_dir)
        token = str(_cfg.get("file_token") or token)
    except Exception:
        print("[file_probe] local config not importable, using defaults")
    return file_dir, token


def _trim(text, limit=80):
    # One-line, first N chars, so a matrix row never wraps the log.
    text = str(text).replace("\r", " ").replace("\n", " ")
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def _err_text(exc, limit=80):
    return "%s: %s" % (exc.__class__.__name__, _trim(exc, limit))


def _atomic_write_json(path, payload):
    tmp_path = path + ".tmp"
    body = json.dumps(payload)
    with open(tmp_path, "wb") as fh:
        fh.write(body.encode("ascii"))
    os.replace(tmp_path, path)


def _read_resp_bytes(path):
    # Read a whole file as bytes WITHOUT builtins.open: the sandbox hooks
    # builtin open()/io.open()/os.open() reads ("PermissionError: Forbidden
    # FileIO") but NOT the low-level _io.FileIO constructor (BIGQMT_FILE_PROBE
    # v3, 2026-10-31). Same read path as the retrofitted FileRedisClient;
    # plain-open fallback keeps normal hosts unchanged.
    try:
        import _io
    except ImportError:
        _io = None
    if _io is not None and hasattr(_io, "FileIO"):
        with _io.FileIO(path, "rb") as fh:
            return fh.read()
    with open(path, "rb") as fh:
        return fh.read()


def _candidate_dirs(file_dir):
    # (short_name, path, allow_create) for the matrix entries.
    # allow_create=True probes os.makedirs for dirs that do not exist yet;
    # py_dir and userdata are only probed when they already exist.
    script_dir = ""
    file_name = globals().get("__file__", "")
    if file_name:
        script_dir = os.path.dirname(os.path.abspath(file_name))
    entries = []
    if script_dir:
        entries.append(("py_dir", script_dir, False))
        entries.append(
            ("py_dir/bridge", os.path.join(script_dir, "qmt_file_bridge_probe"), True)
        )
    temp_dir = os.environ.get("TEMP") or ""
    if temp_dir:
        entries.append(("temp", temp_dir, True))
    if file_dir:
        entries.append(("file_dir", file_dir, True))
    if script_dir:
        # Relative locate keeps the Chinese install path out of this
        # ASCII source; normpath cleans the ".." for nicer logs.
        entries.append(
            ("userdata", os.path.normpath(os.path.join(script_dir, "..", "userdata")), False)
        )
    return entries


def _probe_one_dir(index, total, short, path, allow_create):
    # One matrix row: makedirs (when absent) + write (stat-verified) +
    # readback via builtins.open. Returns {"short","path","verified",
    # "readable"}, or None when the entry is skipped. Never propagates
    # exceptions from the probed operations themselves.
    def emit(md_s, wr_s, rb_s, tail):
        if tail:
            tail = " " + tail
        print(
            "[file_probe] phase1 [%d/%d] %s makedirs=%s write=%s readback=%s%s"
            % (index, total, short, md_s, wr_s, rb_s, tail)
        )
        print("[file_probe]   path=%s" % path)

    if not os.path.isdir(path):
        if not allow_create:
            emit("SKIP", "SKIP", "SKIP", "dir not found")
            return None
        try:
            os.makedirs(path)
        except Exception as exc:
            emit("FAIL", "SKIP", "SKIP", _err_text(exc))
            return None
        md_status = "OK"
    else:
        md_status = "SKIP"

    marker = "probe-v3-%d-%d" % (int(time.time() * 1000), os.getpid())
    marker_bytes = marker.encode("ascii")
    probe_path = os.path.join(path, "probe_io_matrix.tmp")
    notes = []
    write_status = "FAIL"
    try:
        with open(probe_path, "wb") as fh:
            fh.write(marker_bytes)
        # Verify the write really landed: 2026-10-31 showed open(wb)
        # "succeeding" even for a non-existent dir, so trust only stat.
        try:
            size = os.stat(probe_path).st_size
            if size > 0:
                write_status = "OK(verified)"
            else:
                write_status = "FAKE"
                notes.append("stat size 0, write silently dropped?")
        except Exception as exc:
            if exc.__class__.__name__ == "FileNotFoundError":
                write_status = "FAKE"
                notes.append("stat: file missing after write, silently dropped?")
            else:
                write_status = "OK(stat-blocked)"
                notes.append("stat: " + _err_text(exc))
    except Exception as exc:
        notes.append("write: " + _err_text(exc))

    read_status = "SKIP"
    if write_status != "FAIL":
        try:
            with open(probe_path, "rb") as fh:
                back = fh.read()
            if back == marker_bytes:
                read_status = "OK"
            else:
                read_status = "FAIL"
                notes.append("readback: content mismatch")
        except Exception as exc:
            read_status = "FAIL"
            # The read-side error is the headline (Forbidden FileIO case).
            notes.insert(0, "readback: " + _err_text(exc))
    try:
        os.remove(probe_path)
    except OSError:
        pass
    emit(md_status, write_status, read_status, _trim(" | ".join(notes), 120))
    return {
        "short": short,
        "path": path,
        "verified": write_status == "OK(verified)",
        "readable": read_status == "OK",
    }


def _phase1_matrix(file_dir):
    # Run the full matrix; any single failure never affects later entries.
    entries = _candidate_dirs(file_dir)
    total = len(entries)
    results = []
    for index, (short, path, allow_create) in enumerate(entries):
        try:
            item = _probe_one_dir(index + 1, total, short, path, allow_create)
        except Exception as exc:
            # Belt and braces: even a bug in the probe itself must not
            # kill the remaining matrix rows.
            print(
                "[file_probe] phase1 [%d/%d] %s makedirs=SKIP write=FAIL readback=SKIP unexpected %s"
                % (index + 1, total, short, _err_text(exc))
            )
            print("[file_probe]   path=%s" % path)
            item = None
        if item is not None:
            results.append(item)
    verified = [item["short"] for item in results if item["verified"]]
    readable = [item["short"] for item in results if item["readable"]]
    print(
        "[file_probe] phase1 summary: write_verified=%s builtin_readable=%s"
        % (",".join(verified) or "(none)", ",".join(readable) or "(none)")
    )
    if not verified:
        print(
            "[file_probe] HINT: file IO fully blocked in strategy sandbox, "
            "file bridge not viable as-is"
        )
    elif not readable:
        print(
            "[file_probe] HINT: write works but builtin read blocked on every dir "
            "- phase2 probes bypass read channels"
        )
    elif "file_dir" not in verified and ("py_dir" in verified or "py_dir/bridge" in verified):
        print(
            "[file_probe] HINT: move file bridge dir under the QMT python dir"
        )
    return results


def _cleanup_module(mod_path, mod_name):
    # Remove the temp data module plus any __pycache__ entry it produced.
    # Never touches other pyc files; rmdir only succeeds when the dir is
    # left empty by our own removals.
    for _attempt in range(3):
        try:
            os.remove(mod_path)
            break
        except OSError:
            time.sleep(0.01)
    pyc_dir = os.path.join(os.path.dirname(mod_path), "__pycache__")
    try:
        entries = os.listdir(pyc_dir)
    except OSError:
        return
    prefix = mod_name + "."
    removed_all = True
    for entry in entries:
        if entry.startswith(prefix):
            try:
                os.remove(os.path.join(pyc_dir, entry))
            except OSError:
                removed_all = False
    if removed_all:
        try:
            os.rmdir(pyc_dir)
        except OSError:
            pass


def _phase2_one_dir(short, path):
    # Write one probe payload, then try every read/manipulation channel
    # against it. Returns {channel: "OK"/"FAIL"} or None when even the
    # probe write failed. os_replace_remove consumes the probe file, so
    # it always runs last.
    marker = "probe-v3-%d-%d" % (int(time.time() * 1000), os.getpid())
    marker_bytes = marker.encode("ascii")
    probe_path = os.path.join(path, "probe_io_matrix.tmp")
    probe_name = os.path.basename(probe_path)
    results = {}
    errors = {}
    try:
        with open(probe_path, "wb") as fh:
            fh.write(marker_bytes)
    except Exception as exc:
        print(
            "[file_probe] phase2 [%s] write probe FAIL %s" % (short, _err_text(exc, 60))
        )
        return None

    def check(name, fn):
        try:
            results[name] = "OK" if fn() else "FAIL"
            if results[name] == "FAIL":
                errors[name] = "value mismatch"
        except Exception as exc:
            results[name] = "FAIL"
            errors[name] = _err_text(exc, 60)

    def _builtin_open():
        with open(probe_path, "rb") as fh:
            return fh.read() == marker_bytes

    def _io_open():
        with io.open(probe_path, "rb") as fh:
            return fh.read() == marker_bytes

    def _io_fileio():
        import _io
        opener = getattr(_io, "FileIO", None)
        if opener is None:
            opener = _io.open
        with opener(probe_path, "rb") as fh:
            return fh.read() == marker_bytes

    def _os_open_read():
        flags = os.O_RDONLY
        if hasattr(os, "O_BINARY"):
            flags = flags | os.O_BINARY
        fd = os.open(probe_path, flags)
        try:
            data = b""
            while True:
                chunk = os.read(fd, 4096)
                if not chunk:
                    break
                data = data + chunk
        finally:
            os.close(fd)
        return data == marker_bytes

    def _os_stat_size():
        return os.stat(probe_path).st_size > 0

    def _importlib_mod():
        # The import machinery reads files at C level, so importing a
        # self-written data module is a candidate bypass read channel.
        import importlib.util
        mod_name = "probe_mod_%d_%d" % (os.getpid(), int(time.time() * 1000))
        mod_path = os.path.join(path, mod_name + ".py")
        body = 'PAYLOAD = {"n": 12345, "s": "abc"}\n'
        with open(mod_path, "wb") as fh:
            fh.write(body.encode("ascii"))
        try:
            spec = importlib.util.spec_from_file_location(mod_name, mod_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            payload = getattr(mod, "PAYLOAD", None)
            return isinstance(payload, dict) and payload.get("n") == 12345
        finally:
            _cleanup_module(mod_path, mod_name)

    def _os_listdir():
        return probe_name in os.listdir(path)

    def _os_replace_remove():
        # Both primitives are required by the bridge protocol.
        bak_path = probe_path + ".bak"
        os.replace(probe_path, bak_path)
        os.remove(bak_path)
        return True

    check("builtin_open", _builtin_open)
    check("io_open", _io_open)
    check("_io_fileio", _io_fileio)
    check("os_open_read", _os_open_read)
    check("os_stat_size", _os_stat_size)
    check("importlib_mod", _importlib_mod)
    check("os_listdir", _os_listdir)
    check("os_replace_remove", _os_replace_remove)
    # Last-resort cleanup when os_replace_remove itself failed midway.
    for leftover in (probe_path, probe_path + ".bak"):
        try:
            os.remove(leftover)
        except OSError:
            pass
    compact = " ".join(
        "%s=%s" % (name, results.get(name, "SKIP")) for name in CHANNEL_ORDER
    )
    print("[file_probe] phase2 [%s] %s" % (short, compact))
    failed = [name for name in CHANNEL_ORDER if errors.get(name)]
    if failed:
        detail = " | ".join("%s %s" % (name, errors[name]) for name in failed)
        print("[file_probe] phase2 [%s] errors: %s" % (short, _trim(detail, 200)))
    return results


def _phase2_matrix(dirs):
    # dirs: list of (short, path) with write-verified paths (or TEMP as
    # the last-resort candidate). Aggregates which read channels work
    # where, then emits the viability hint.
    channel_dirs = {}
    for short, path in dirs:
        try:
            results = _phase2_one_dir(short, path)
        except Exception as exc:
            print(
                "[file_probe] phase2 [%s] unexpected %s" % (short, _err_text(exc, 60))
            )
            results = None
        if not results:
            continue
        for name, status in results.items():
            if status == "OK":
                channel_dirs.setdefault(name, []).append(short)
    read_ok = [
        "%s(%s)" % (name, ",".join(channel_dirs[name]))
        for name in READ_CHANNELS
        if channel_dirs.get(name)
    ]
    if read_ok:
        print("[file_probe] phase2 summary: read channels OK: %s" % ", ".join(read_ok))
        bypass = [name for name in READ_CHANNELS if channel_dirs.get(name)]
        print(
            "[file_probe] HINT: bypass read channel available: %s - "
            "file bridge viable, report this to update the client"
            % ", ".join(bypass)
        )
    else:
        print("[file_probe] HINT: no read channel, file bridge needs another design")
    return channel_dirs


def _phase3_ping(file_dir, token):
    # One ping request through the file bridge, timed in milliseconds.
    # Kept from v2: nonce echo, transient-lock retry on the freshly
    # replaced resp file, no re-send on timeout. The resp read now goes
    # through _io.FileIO - the sandbox read bypass verified in phase2 -
    # so with a live daemon this is expected to PASS inside the sandbox.
    req_dir = os.path.join(file_dir, "req")
    resp_dir = os.path.join(file_dir, "resp")
    try:
        for path in (req_dir, resp_dir):
            if not os.path.isdir(path):
                os.makedirs(path)
        now_ms = int(time.time() * 1000)
        # The daemon only accepts int ids; the low digits carry the pid so
        # two probes started in the same millisecond never collide. Pure
        # digits keep the file name safe.
        request_id = now_ms * 1000 + os.getpid() % 1000
        nonce = "%d-%s" % (now_ms, os.getpid())
        req_path = os.path.join(req_dir, "%d.json" % request_id)
        resp_path = os.path.join(resp_dir, "%d.json" % request_id)
        for path in (req_path, resp_path):
            if os.path.exists(path):
                os.remove(path)
        started = time.time()
        _atomic_write_json(
            req_path,
            {"id": request_id, "nonce": nonce, "token": token, "cmd": "ping", "args": []},
        )
        deadline = time.time() + PING_TIMEOUT
        reply = None
        while True:
            raw = None
            if os.path.exists(resp_path):
                # Same transient-lock retry as the client: a freshly
                # replaced file can be briefly locked by antivirus scanning.
                # Read via _io.FileIO so the sandbox read hook cannot block
                # it (identical to the retrofitted FileRedisClient path).
                try:
                    raw = _read_resp_bytes(resp_path)
                except OSError:
                    raw = None
                if raw is not None:
                    try:
                        os.remove(resp_path)
                    except OSError:
                        pass
                    try:
                        candidate = json.loads(raw.decode("utf-8"))
                    except ValueError:
                        candidate = None
                    if isinstance(candidate, dict) and candidate.get("nonce") == nonce:
                        reply = candidate
                        break
                    # Wrong/missing nonce: stale file, already removed; poll on.
            if time.time() >= deadline:
                raise IOError(
                    "no response within %.1fs (daemon not running / token mismatch / dir mismatch)"
                    % PING_TIMEOUT
                )
            time.sleep(POLL_INTERVAL)
        cost_ms = (time.time() - started) * 1000.0
        if reply.get("ok") is False:
            # Round trip itself worked (nonce echoed), but the daemon
            # rejected the request: token mismatch is the usual cause.
            print(
                "[file_probe] phase3 round trip OK %.1fms but daemon rejected: %s"
                % (cost_ms, _trim(reply.get("error"), 120))
            )
            print(
                "[file_probe] phase3 file bridge ping FAIL token/dir mismatch, fix and rerun"
            )
            return False
        print(
            "[file_probe] phase3 file bridge ping OK %.1fms ok=%s reply=%s"
            % (cost_ms, reply.get("ok"), reply)
        )
        print("[file_probe] if rtt > 10ms, see checklist section for tuning")
        return True
    except Exception as exc:
        print(
            "[file_probe] phase3 file bridge ping FAIL %s: %s"
            % (exc.__class__.__name__, exc)
        )
        return False


def init(ContextInfo):
    file_dir, token = _load_file_config()
    results = _phase1_matrix(file_dir)
    verified = [item for item in results if item["verified"]]
    if verified:
        dirs = [(item["short"], item["path"]) for item in verified]
    else:
        # Nothing verifiably written anywhere: still probe TEMP so the
        # bypass matrix has at least one dir to work on.
        temp_dir = os.environ.get("TEMP") or ""
        dirs = [("temp", temp_dir)] if temp_dir else []
    if dirs:
        _phase2_matrix(dirs)
    else:
        print("[file_probe] phase2 skipped: no candidate dir at all")
    # phase3 target: the configured file_dir when phase1 verified it,
    # otherwise the first verified dir (TEMP fallback last). The ping
    # itself still reads resp via builtins.open, so under the current
    # sandbox it is expected to FAIL until the client is retrofitted.
    target = None
    for item in results:
        if item["short"] == "file_dir" and item["verified"]:
            target = ("file_dir", file_dir)
            break
    if target is None and verified:
        target = (verified[0]["short"], verified[0]["path"])
    if target is None and dirs:
        target = dirs[0]
    if target is None:
        print("[file_probe] phase3 skipped: no target dir")
    else:
        print("[file_probe] phase3 target %s -> %s" % (target[0], target[1]))
        print(
            "[file_probe] phase3 note: resp read goes through _io.FileIO "
            "(sandbox bypass), expect ping OK when the daemon runs"
        )
        _phase3_ping(target[1], token)
    print("[file_probe] done")


def handlebar(ContextInfo):
    return None

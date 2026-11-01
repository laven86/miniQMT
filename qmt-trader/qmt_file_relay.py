# -*- coding: utf-8 -*-
"""文件桥守护进程：用文件交换代替 TCP 中继，供大QMT 策略执行 Redis 命令。

背景：券商 2026-10-23 升级后，大QMT 主客户端周期性枚举内嵌策略进程的
TCP 连接表，任何对端不在券商白名单——包括 127.0.0.1 回环——都会触发
onIllegalIPFind「不可用IP」并强制停止所有策略。因此原 TCP 中继方案
（已废弃删除，共享逻辑见 qmt_relay_common.py）与直连 6379 均被拦，
一切 TCP/UDP/socket 方案不可行。策略进程内文件读写仍可用，唯一可行
通道是文件交换。

本进程运行在普通 Python 里（不受限制），持有 Redis 连接；大QMT 策略端
（adapters/file_redis_client.py，纯标准库、零 socket）把 Redis 命令写成
请求文件，本守护进程轮询读取、代为执行、把响应写成响应文件。

目录布局（默认 <本目录>/qmt_file_bridge，--dir 或 QMT_FILE_DIR 可覆盖）：
    <root>/req/    请求文件 <id>.json（客户端原子写入）
    <root>/resp/   响应文件 <id>.json（本进程原子写入）

帧格式（沿用原中继的 JSON 结构并增加 nonce）：
    请求 {"id": <int递增>, "nonce": "<每进程唯一>", "token": "<str>", "cmd": "<str>", "args": [...]}
    响应 {"id": <int>, "nonce": "<原样回显>", "ok": true, "data": ...} 或
         {"id": <int>, "nonce": "<原样回显>", "ok": false, "error": "<msg>"}
    bytes 值用 {"__b64__": ...} 包裹；请求侧 dict 用 {"__map__": [[k, v], ...]}
    包裹（键为字符串），响应侧 dict（如 hgetall 的 bytes 键）同样用 __map__。
    id 必须是 int（bool 除外）——响应文件名由 id 拼接，非 int 一律拒绝，
    堵掉路径注入；nonce 由客户端每进程生成，响应原样回显，客户端校验
    不一致即忽略，防止重启后错认旧响应。

安全：与原 TCP 中继完全一致——命令白名单、key 前缀校验（默认 bigqmt:）、
共享 token 鉴权。白名单、校验与执行逻辑直接 import 自 qmt_relay_common.py，
不做第二份实现。另外两道防护：
    - 单实例锁：root 下以 O_CREAT|O_EXCL 创建 qmt_file_relay.lock，
      重复启动立即失败退出；持锁进程已死（残留锁）时自动清锁重试一次；
    - 主循环逐文件兑底异常：单条坏文件绝不杀死桥。
"""

import argparse
import json
import os
import threading
import time
import traceback

import qmt_relay_common as relay

DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "qmt_file_bridge")


class FileRelayLockedError(RuntimeError):
    """root 下已有另一个守护进程实例持有锁文件。"""


def _remove_file(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _read_with_retry(path, attempts=10, delay=0.005):
    """读文件带重试：Windows 上新建文件可能被杀软/索引器短暂锁定。"""
    for _ in range(attempts):
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except OSError:
            time.sleep(delay)
    return None


def _remove_with_retry(path, attempts=50, delay=0.02):
    """删除文件带重试；删不掉时返回 False（调用方须防止重扫重放）。"""
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
    """先写 <path>.tmp 再 os.replace 到最终名（Windows 同卷原子）。

    读取方只认最终名，忽略 .tmp，因此不会读到半个文件。
    ensure_ascii=True 让文件内容保持纯 ASCII。
    """
    tmp_path = path + ".tmp"
    body = json.dumps(payload)
    with open(tmp_path, "wb") as fh:
        fh.write(body.encode("ascii"))
    os.replace(tmp_path, path)


def _normalize_request_value(value):
    """把请求侧 {"__map__": [[k, v], ...]} 还原成普通 dict。

    键保持字符串（JSON 对象键只能是字符串，客户端的 _wrap_key 已保证），
    值可能嵌套 __map__/__b64__，递归处理。还原成普通 dict 后，其余解码
    （__b64__ 等）交给 relay.validate_request() 里的 decode_value 统一做。
    """
    if isinstance(value, dict):
        if set(value.keys()) == {"__map__"} and isinstance(value["__map__"], list):
            merged = {}
            for pair in value["__map__"]:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise relay.RelayError("bad __map__ pair: %r" % (pair,))
                merged[pair[0]] = pair[1]
            return {key: _normalize_request_value(item) for key, item in merged.items()}
        return {key: _normalize_request_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_request_value(item) for item in value]
    return value


def _read_lock_pid(lock_path):
    """从锁文件内容（"pid=<n> started=..."）解析出 pid；解析不了返回 None。"""
    try:
        with open(lock_path, "rb") as fh:
            content = fh.read(200).decode("ascii", "replace")
    except OSError:
        return None
    for part in content.split():
        if part.startswith("pid="):
            try:
                return int(part[4:])
            except ValueError:
                return None
    return None


def _pid_alive(pid):
    """判断 pid 进程是否存活。

    Windows 上不能用 os.kill(pid, 0)：CPython 在 Windows 把非 CTRL_* 信号
    直接走 TerminateProcess——sig=0 也会真杀进程，绝不可用。可靠方式是
    ctypes 调 OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION) +
    GetExitCodeProcess：句柄拿不到（进程已退出且无人持有句柄，
    GetLastError=ERROR_INVALID_PARAMETER）判死；拿到句柄但退出码
    != STILL_ACTIVE(259) 也判死（僵尸句柄场景）。探测本身抛异常时按
    "活"处理——宁可不自愈也不误删活锁（保守方向：误判死会删掉正在
    工作实例的锁，破坏单实例语义；误判活只是多打一行告警人工处理）。
    非 Windows 平台用 POSIX os.kill(pid, 0)。
    """
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            handle = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
            )
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                if ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return int(exit_code.value) == STILL_ACTIVE
                return True  # 拿到退出码失败：按活处理（保守）
            finally:
                try:
                    ctypes.windll.kernel32.CloseHandle(handle)
                except Exception:
                    pass
        except Exception:
            return True  # 探测失败：按活处理（保守）
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False
    return False


class FileRelayDaemon(object):
    """轮询 req/ 目录，串行执行 Redis 命令并把响应写入 resp/ 目录。

    - 启动时（prepare）清空 req/ 与 resp/ 全部残留文件：客户端进程重启后
      id 计数器从 1 重新递增，旧响应文件若不清掉会被误当成新响应。
    - 主循环每 poll_interval（默认 2ms）扫描一次 req/*.json；处理完一批
      立即再扫（突发请求不额外等待），空转才 sleep。
    - 校验失败（token/白名单/前缀/JSON 损坏/非 int id）同样写 {"ok": false}
      响应，让客户端立即拿到明确错误。
    - 单实例：serve_forever 先在 root 下抢 O_CREAT|O_EXCL 锁文件
      （qmt_file_relay.lock），抢不到抛 FileRelayLockedError；退出时释放
      删除。prepare() 只清 req/ resp/，从不碰 root 下的锁文件。
    - 锁自愈：抢锁失败时读锁内 pid，用 Windows API 判断进程存活，
      死进程则删残留锁重试一次（见 _acquire_lock）——独立运行与嵌入
      main.py 线程化（daemon 线程硬退不清锁）都受益。
    """

    def __init__(self, root_dir, redis_client, token="", key_prefix="bigqmt:", poll_interval=0.002):
        self.root_dir = os.path.abspath(str(root_dir))
        self.req_dir = os.path.join(self.root_dir, "req")
        self.resp_dir = os.path.join(self.root_dir, "resp")
        self.redis = redis_client
        self.token = str(token or "")
        self.key_prefix = str(key_prefix or "")
        self.poll_interval = float(poll_interval or 0.002)
        self._stop = threading.Event()
        self.ready = threading.Event()
        self._quarantine = set()
        self.lock_path = os.path.join(self.root_dir, "qmt_file_relay.lock")
        self._lock_handle = None

    def prepare(self):
        """创建 req/ resp/ 目录，并清空两侧全部残留文件（含 .tmp）。"""
        for directory in (self.req_dir, self.resp_dir):
            if not os.path.isdir(directory):
                os.makedirs(directory)
        for directory in (self.req_dir, self.resp_dir):
            for name in os.listdir(directory):
                _remove_file(os.path.join(directory, name))

    def serve_forever(self):
        self._acquire_lock()
        try:
            self.prepare()
            self.ready.set()
            print(
                "[qmt_file_relay] watching %s (req/ resp/) prefix=%r"
                % (self.root_dir, self.key_prefix)
            )
            while not self._stop.is_set():
                try:
                    handled = self._poll_once()
                except Exception:
                    # 兜底：扫描本身出错（如目录瞬时不可读）也不退出。
                    traceback.print_exc()
                    handled = False
                if not handled:
                    self._stop.wait(self.poll_interval)
        finally:
            self.ready.clear()
            self._release_lock()

    def _acquire_lock(self):
        """在 root 下创建 O_CREAT|O_EXCL 锁文件保证单实例；句柄进程内常开。

        抢锁失败（残留锁）时自愈一次：读锁内 pid 判存活，持锁进程已死
        （典型场景：嵌入 main.py 的 daemon 线程随进程硬退、finally 没跑）
        则删锁重试一次；进程仍活或重试仍失败才抛 FileRelayLockedError。
        """
        if self._lock_handle is not None:
            return
        if not os.path.isdir(self.root_dir):
            os.makedirs(self.root_dir)
        try:
            handle = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            handle = self._try_reclaim_stale_lock()
            if handle is None:
                existing = ""
                try:
                    with open(self.lock_path, "rb") as fh:
                        existing = fh.read(200).decode("ascii", "replace")
                except OSError:
                    pass
                raise FileRelayLockedError(
                    "another qmt_file_relay instance is already watching %s "
                    "(lock file %s, content %r); if no daemon is running, "
                    "delete the lock file" % (self.root_dir, self.lock_path, existing)
                )
        except OSError as exc:
            raise FileRelayLockedError("cannot create lock file %s: %s" % (self.lock_path, exc))
        try:
            os.write(
                handle,
                ("pid=%d started=%s" % (os.getpid(), time.strftime("%Y-%m-%d %H:%M:%S"))).encode("ascii"),
            )
        except OSError:
            pass
        self._lock_handle = handle

    def _try_reclaim_stale_lock(self):
        """持锁进程已死时删残留锁并重抢一次；返回新句柄或 None（不自愈）。"""
        stale_pid = _read_lock_pid(self.lock_path)
        if stale_pid is None or _pid_alive(stale_pid):
            return None
        print(
            "[qmt_file_relay] stale lock %s (pid=%d is dead), reclaiming"
            % (self.lock_path, stale_pid)
        )
        try:
            os.remove(self.lock_path)
        except OSError:
            return None
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except OSError:
            return None

    def _release_lock(self):
        handle = self._lock_handle
        self._lock_handle = None
        if handle is None:
            return
        try:
            os.close(handle)
        except OSError:
            pass
        try:
            os.remove(self.lock_path)
        except OSError:
            pass

    def stop(self):
        self._stop.set()

    def _poll_once(self):
        try:
            names = os.listdir(self.req_dir)
        except OSError:
            return False
        handled = False
        for name in sorted(names):
            if self._stop.is_set():
                break
            if not name.endswith(".json") or name in self._quarantine:
                continue
            try:
                self._handle_request(name)
            except Exception:
                # 兜底：单条坏文件绝不杀死桥，打印后继续处理下一条。
                traceback.print_exc()
            handled = True
        return handled

    def _handle_request(self, name):
        req_path = os.path.join(self.req_dir, name)
        request_id = name[: -len(".json")]
        raw = _read_with_retry(req_path)
        if raw is None:
            # 请求文件完全读不了：写错误响应，别让客户端一直轮询到超时。
            # request_id 退化为文件名（listdir 出来的安全 basename）。
            response = {
                "id": request_id,
                "ok": False,
                "error": "OSError: unreadable request file",
            }
        else:
            nonce = None
            try:
                request = json.loads(raw.decode("utf-8"))
                if not isinstance(request, dict):
                    raise relay.RelayError("request must be a json object")
                # id 必须是 int（bool 除外）：响应文件名由它拼接，非 int
                # 一律拒绝，堵掉 "id": "../../evil" 之类的路径注入。
                frame_id = request.get("id")
                if isinstance(frame_id, bool) or not isinstance(frame_id, int):
                    raise relay.RelayError("request id must be an int")
                request_id = str(frame_id)
                candidate_nonce = request.get("nonce")
                if isinstance(candidate_nonce, str):
                    nonce = candidate_nonce
                request = dict(request)
                request["args"] = _normalize_request_value(request.get("args") or [])
                cmd, args = relay.validate_request(request, self.token, self.key_prefix)
                result = relay.execute(self.redis, cmd, args)
                response = {"id": request_id, "ok": True, "data": relay.encode_value(result)}
            except Exception as exc:
                response = {
                    "id": request_id,
                    "ok": False,
                    "error": "%s: %s" % (exc.__class__.__name__, exc),
                }
            if nonce is not None:
                # 原样回显：客户端据此识别自己的响应，错认旧响应即忽略。
                response["nonce"] = nonce
        self._write_response(request_id, response)
        # 请求文件必须删掉才算处理完：删不掉就绝不能重扫重处理，
        # 否则 LPOP/RPUSH 会被重放。重试无效则拉入隔离名单并报错。
        if not _remove_with_retry(req_path):
            self._quarantine.add(name)
            print(
                "[qmt_file_relay] cannot remove %s; quarantined (will be wiped "
                "on next daemon restart)" % req_path
            )

    def _write_response(self, request_id, response):
        resp_path = os.path.join(self.resp_dir, "%s.json" % request_id)
        _atomic_write_json(resp_path, response)


def main():
    parser = argparse.ArgumentParser(
        description="文件桥守护进程：大QMT策略经文件交换执行 Redis 命令（零 socket）"
    )
    parser.add_argument("--host", default=os.environ.get("QMT_RPC_REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("QMT_RPC_REDIS_PORT", "6379")))
    parser.add_argument("--db", type=int, default=int(os.environ.get("QMT_RPC_REDIS_DB", "5")))
    parser.add_argument("--password", default=os.environ.get("QMT_RPC_REDIS_PASSWORD", ""))
    parser.add_argument("--dir", default=os.environ.get("QMT_FILE_DIR", DEFAULT_DIR))
    parser.add_argument("--token", default=os.environ.get("QMT_FILE_TOKEN", ""))
    parser.add_argument("--key-prefix", default=os.environ.get("QMT_FILE_KEY_PREFIX", "bigqmt:"))
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("QMT_FILE_POLL_INTERVAL", "0.002")),
    )
    args = parser.parse_args()

    client = relay.build_redis_client(args.host, args.port, args.db, args.password)
    for attempt in range(1, 11):
        try:
            client.ping()
            break
        except Exception as exc:
            print("[qmt_file_relay] redis not ready (%s), retry %d/10" % (exc, attempt))
            time.sleep(1.5)
    else:
        raise SystemExit("[qmt_file_relay] cannot connect to redis")

    daemon = FileRelayDaemon(
        args.dir,
        client,
        token=args.token,
        key_prefix=args.key_prefix,
        poll_interval=args.poll_interval,
    )
    try:
        daemon.serve_forever()
    except KeyboardInterrupt:
        daemon.stop()
    except FileRelayLockedError as exc:
        print("[qmt_file_relay] %s" % exc)
        raise SystemExit(2)


if __name__ == "__main__":
    main()

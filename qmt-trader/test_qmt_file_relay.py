# -*- coding: utf-8 -*-
"""qmt_file_relay 端到端测试：真实 Redis + 文件桥守护进程 + FileRedisClient。

覆盖策略端实际用到的每条命令、bytes 往返保真（含非 ASCII bytes）、token
与 key 前缀拒绝、白名单外命令拒绝、客户端超时与残留响应清理、守护进程
启动清理、build_redis_client 的 file_dir 优先级，以及完整 RPC 闭环。
运行需要本机 Redis（默认 127.0.0.1:6379，可用 QMT_RPC_REDIS_* 覆盖）。
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

import redis

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(_HERE, "vender", "bigqmt", "src")
sys.path.insert(0, _HERE)
sys.path.insert(0, _SRC)

import qmt_file_relay as file_relay
from bigqmt_signal_trader.adapters.file_redis_client import (
    FileRedisClient,
    FileRelayError,
    FileRelayTimeoutError,
)
from bigqmt_signal_trader.adapters.redis_common import build_redis_client

REDIS_HOST = os.environ.get("QMT_RPC_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("QMT_RPC_REDIS_PORT", "6379"))
REDIS_DB = 15
TOKEN = "test-token"
PREFIX = "bigqmt:"


def _redis_available():
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=1.0)
        return bool(client.ping())
    except Exception:
        return False


def _atomic_write(path, data):
    """测试内注入文件也走 tmp+os.replace，防止桥读到半写状态。"""
    tmp_path = path + ".tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(data)
    os.replace(tmp_path, path)


@unittest.skipUnless(_redis_available(), "local redis not reachable")
class FileRelayRoundTripTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=2.0)
        cls.backend.flushdb()
        cls.root_dir = tempfile.mkdtemp(prefix="qmt_file_relay_test_")
        cls.daemon = file_relay.FileRelayDaemon(
            cls.root_dir, cls.backend, token=TOKEN, key_prefix=PREFIX
        )
        cls.thread = threading.Thread(target=cls.daemon.serve_forever, name="file-relay-test", daemon=True)
        cls.thread.start()
        cls.assertTrue(cls.daemon.ready.wait(5), "daemon did not become ready")
        cls.client = FileRedisClient(cls.root_dir, token=TOKEN, timeout=5.0)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.daemon.stop()
        cls.thread.join(2)
        cls.backend.flushdb()
        shutil.rmtree(cls.root_dir, ignore_errors=True)

    # -- 命令往返与 bytes 保真 ---------------------------------------------------

    def test_string_commands_roundtrip_bytes(self):
        key = "bigqmt:positions:acc"
        self.assertTrue(self.client.set(key, b'{"qty": 1}'))
        self.assertEqual(self.client.get(key), b'{"qty": 1}')
        # 非 ASCII bytes 也必须原样往返（经 __b64__ 编码）。
        cjk = "中文负载".encode("utf-8")
        self.assertTrue(self.client.set(key, cjk))
        self.assertEqual(self.client.get(key), cjk)
        self.assertTrue(self.client.setex(key, 30, b"v2"))
        self.assertEqual(self.client.get(key), b"v2")
        self.assertGreater(self.backend.ttl(key), 0)
        self.assertEqual(self.client.delete(key), 1)
        self.assertIsNone(self.client.get(key))

    def test_list_queue_matches_rpc_drain_path(self):
        key = "bigqmt:rpc:queue:acc"
        self.assertEqual(self.client.rpush(key, b"req-1", b"req-2"), 2)
        self.assertTrue(self.client.expire(key, 60))
        self.assertEqual(self.client.lpop(key), b"req-1")
        self.assertEqual(self.client.lpop(key), b"req-2")
        self.assertIsNone(self.client.lpop(key))

    def test_hash_and_publish(self):
        key = "bigqmt:signal_status:acc:sig"
        self.assertEqual(self.client.hset(key, mapping={"status": "CLAIMED", "qty": "1"}), 2)
        result = self.client.hgetall(key)
        self.assertEqual(result[b"status"], b"CLAIMED")
        self.assertEqual(self.client.hgetall("bigqmt:missing:acc"), {})
        self.assertIsInstance(self.client.publish("bigqmt:order_events:acc", b"payload"), int)

    def test_set_nx_ex_claim_semantics(self):
        key = "bigqmt:signal_claim:acc:sig"
        self.assertTrue(self.client.set(key, "consumer-a", nx=True, ex=30))
        self.assertIsNone(self.client.set(key, "consumer-b", nx=True, ex=30))
        self.assertEqual(self.backend.get(key), b"consumer-a")
        self.assertGreater(self.backend.ttl(key), 0)

    def test_xadd_and_ping(self):
        entry_id = self.client.xadd("bigqmt:signals:acc", {"payload": "hello"})
        self.assertIsInstance(entry_id, bytes)
        self.assertTrue(self.client.ping())

    # -- 安全拒绝 ---------------------------------------------------------------

    def test_bad_token_is_rejected(self):
        bad = FileRedisClient(self.root_dir, token="nope")
        try:
            with self.assertRaises(FileRelayError) as caught:
                bad.get("bigqmt:positions:acc")
            self.assertIn("bad token", str(caught.exception))
        finally:
            bad.close()
        # 守护进程处理完（写错误响应、删请求文件）后 req/ 不留残留。
        req_dir = os.path.join(self.root_dir, "req")
        deadline = time.time() + 2
        while time.time() < deadline and os.listdir(req_dir):
            time.sleep(0.01)
        self.assertEqual(os.listdir(req_dir), [])

    def test_key_outside_prefix_is_rejected(self):
        with self.assertRaises(FileRelayError):
            self.client.get("other:key")
        with self.assertRaises(FileRelayError):
            self.client.delete("FLUSHALL")

    def test_disallowed_command_never_reaches_redis(self):
        self.client._execute("get", ["bigqmt:positions:acc"])
        with self.assertRaises(FileRelayError) as caught:
            self.client._execute("flushdb", [])
        self.assertIn("not allowed", str(caught.exception))
        # flushdb 确实没有执行：db 里的既有 key 还在。
        self.backend.set("bigqmt:survivor", b"alive")
        self.assertEqual(self.backend.get("bigqmt:survivor"), b"alive")

    # -- 超时与残留文件清理 -------------------------------------------------------

    def test_timeout_removes_own_request_file(self):
        """桥不响应（本测试目录无守护进程）时按超时抛错，不重试不挂死。

        超时自删：客户端放弃本次调用时删除自己写的 req/<id>.json，
        防止桥离线期间 req/ 无限堆积（实盘曾堆 2700+ 个）。
        """
        quiet_dir = os.path.join(self.root_dir, "quiet")
        client = FileRedisClient(quiet_dir, token=TOKEN, timeout=0.1)
        req_dir = os.path.join(quiet_dir, "req")
        try:
            started = time.time()
            with self.assertRaises(FileRelayTimeoutError):
                client.ping()
            self.assertLess(time.time() - started, 1.0)
            # 超时后请求文件已被客户端自删，req/ 不留残留。
            self.assertFalse(os.path.exists(os.path.join(req_dir, "1.json")))
            # 超时不重试：锁已释放，下一次请求是新 id；多次调用也不会堆积。
            with self.assertRaises(FileRelayTimeoutError):
                client.ping()
            with self.assertRaises(FileRelayTimeoutError):
                client.ping()
            self.assertEqual(os.listdir(req_dir), [])
        finally:
            client.close()

    def test_late_response_cleaned_on_startup_and_per_request(self):
        """迟到的响应文件：启动时全量清理 + 每次请求前清理当前 id。"""
        quiet_dir = os.path.join(self.root_dir, "late")
        stale = os.path.join(quiet_dir, "resp", "1.json")
        # 场景1：上一进程超时后，响应迟到留下残留文件；新客户端启动即清掉。
        os.makedirs(os.path.join(quiet_dir, "resp"))
        with open(stale, "wb") as fh:
            fh.write(b'{"id": 1, "ok": true, "data": true}')
        fresh = FileRedisClient(quiet_dir, token=TOKEN, timeout=0.1)
        try:
            self.assertFalse(os.path.exists(stale))
            # 场景2：进程未重启时残留出现在即将使用的 id 上——请求前清理，
            # 不会被误当成新响应（这里无守护进程，仍应超时而不是假成功）。
            with open(stale, "wb") as fh:
                fh.write(b'{"id": 1, "ok": true, "data": true}')
            with self.assertRaises(FileRelayTimeoutError):
                fresh.ping()
            self.assertFalse(os.path.exists(stale))
        finally:
            fresh.close()

    def test_daemon_startup_cleans_residual_files(self):
        dirty_dir = os.path.join(self.root_dir, "dirty")
        req_dir = os.path.join(dirty_dir, "req")
        resp_dir = os.path.join(dirty_dir, "resp")
        os.makedirs(req_dir)
        os.makedirs(resp_dir)
        for name, body in (
            (os.path.join(req_dir, "99.json"), b"stale request"),
            (os.path.join(req_dir, "99.json.tmp"), b"half written"),
            (os.path.join(resp_dir, "42.json"), b"stale response"),
        ):
            with open(name, "wb") as fh:
                fh.write(body)
        daemon = file_relay.FileRelayDaemon(dirty_dir, self.backend, token=TOKEN, key_prefix=PREFIX)
        daemon.prepare()
        try:
            self.assertEqual(os.listdir(req_dir), [])
            self.assertEqual(os.listdir(resp_dir), [])
        finally:
            daemon.stop()

    # -- 评审修复回归：坏文件不杀桥 / nonce / 单实例锁 -----------------------------

    def test_bad_request_files_do_not_kill_daemon(self):
        """坏 JSON / 非 int id（路径注入尝试）/ bool id：桥写错误响应并继续服务。"""
        req_dir = os.path.join(self.root_dir, "req")
        resp_dir = os.path.join(self.root_dir, "resp")

        def wait_response(name, timeout=3.0):
            path = os.path.join(resp_dir, name)
            deadline = time.time() + timeout
            while time.time() < deadline:
                if os.path.exists(path):
                    with open(path, "rb") as fh:
                        raw = fh.read()
                    os.remove(path)
                    return json.loads(raw.decode("utf-8"))
                time.sleep(0.005)
            self.fail("daemon never answered %s" % name)

        # 1) 损坏的 JSON：桥写错误响应，不崩。
        _atomic_write(os.path.join(req_dir, "900.json"), b"{{{not json")
        resp = wait_response("900.json")
        self.assertFalse(resp["ok"])
        self.assertIn("JSONDecodeError", resp["error"])

        # 2) id 非 int：拒绝，且不产生任何路径逃逸文件。
        evil_id = {
            "id": "../../evil",
            "nonce": "evil",
            "token": TOKEN,
            "cmd": "get",
            "args": ["bigqmt:probe:evil"],
        }
        _atomic_write(os.path.join(req_dir, "901.json"), json.dumps(evil_id).encode("ascii"))
        resp = wait_response("901.json")
        self.assertFalse(resp["ok"])
        self.assertIn("id must be an int", resp["error"])
        self.assertFalse(os.path.exists(os.path.join(self.root_dir, "evil.json")))
        self.assertFalse(os.path.exists(os.path.join(self.root_dir, "..", "evil.json")))

        # 3) bool id：同样拒绝（bool 是 int 子类，须显式排除）。
        bool_id = {"id": True, "nonce": "evil", "token": TOKEN, "cmd": "ping", "args": []}
        _atomic_write(os.path.join(req_dir, "902.json"), json.dumps(bool_id).encode("ascii"))
        resp = wait_response("902.json")
        self.assertFalse(resp["ok"])
        self.assertIn("id must be an int", resp["error"])

        # 桥还活着，正常客户端命令继续工作。
        self.assertTrue(self.client.ping())

    def test_wrong_or_missing_nonce_response_is_ignored(self):
        """错 nonce / 缺 nonce 的响应被忽略并清理，客户端按超时抛错。"""
        quiet_dir = os.path.join(self.root_dir, "nonce")
        client = FileRedisClient(quiet_dir, token=TOKEN, timeout=0.4)
        resp_dir = os.path.join(quiet_dir, "resp")

        def attempt(expected_request_id, body):
            # 请求文件出现 = 预清理已过，此时投放的伪响应会进入轮询视线，
            # 只能靠 nonce 校验拒绝。
            req_path = os.path.join(quiet_dir, "req", "%d.json" % expected_request_id)
            resp_path = os.path.join(resp_dir, "%d.json" % expected_request_id)
            outcome = {}

            def call():
                try:
                    client.ping()
                    outcome["result"] = "unexpected-success"
                except FileRelayTimeoutError:
                    outcome["result"] = "timeout"
                except Exception as exc:
                    outcome["result"] = "%s: %s" % (exc.__class__.__name__, exc)

            worker = threading.Thread(target=call)
            worker.start()
            deadline = time.time() + 2
            while time.time() < deadline and not os.path.exists(req_path):
                time.sleep(0.002)
            _atomic_write(resp_path, body)
            worker.join(5)
            self.assertEqual(outcome.get("result"), "timeout")
            # 伪响应已被客户端删除，不会留到下次误认。
            self.assertFalse(os.path.exists(resp_path))

        attempt(1, b'{"id": 1, "nonce": "someone-else", "ok": true, "data": true}')
        attempt(2, b'{"id": 2, "ok": true, "data": true}')
        client.close()

    def test_second_daemon_instance_exits_immediately(self):
        """单实例锁：同 root 的第二个守护进程立即退出，原实例继续服务。"""
        second = file_relay.FileRelayDaemon(
            self.root_dir, self.backend, token=TOKEN, key_prefix=PREFIX
        )
        with self.assertRaises(file_relay.FileRelayLockedError):
            second.serve_forever()
        # 锁文件仍由第一个实例持有，原实例不受影响。
        self.assertTrue(os.path.exists(os.path.join(self.root_dir, "qmt_file_relay.lock")))
        self.assertTrue(self.client.ping())

    # -- 沙箱绕行（2026-10-31 v3 探针实测结论） ---------------------------------

    def test_ping_survives_builtin_open_read_block(self):
        """模拟 QMT 沙箱：builtins.open 读模式抛 Forbidden FileIO（写放行）。

        实测沙箱 hook builtins.open/io.open/os.open 的读路径但不拦
        _io.FileIO；客户端读 resp 已改走 _io.FileIO，ping 往返不应依赖
        builtins.open。守护进程线程不受 patch（真实沙箱只拦策略进程）。
        """
        import builtins

        real_open = builtins.open
        sandbox_ident = threading.get_ident()

        def sandbox_open(file, mode="r", *args, **kwargs):
            # 读模式（'rb'/'r'/默认空）只在本测试线程拦；写/追加模式放行
            # （真实沙箱里写是放行的，请求文件仍要能写出）。
            if threading.get_ident() == sandbox_ident and ("r" in mode or not mode):
                raise PermissionError("Forbidden FileIO")
            return real_open(file, mode, *args, **kwargs)

        builtins.open = sandbox_open
        try:
            self.assertTrue(self.client.ping())
        finally:
            builtins.open = real_open

    def test_read_file_bytes_fallback_when_io_missing(self):
        """_io 不可用时回退 builtins.open：普通环境行为不变。"""
        from bigqmt_signal_trader.adapters import file_redis_client as mod

        payload = b'{"id": 1, "ok": true, "data": true}'
        path = os.path.join(self.root_dir, "resp", "fallback_probe.json")
        _atomic_write(path, payload)
        saved = sys.modules.get("_io")
        sys.modules["_io"] = None  # import _io -> ImportError -> 回退 builtins.open
        try:
            self.assertEqual(mod._read_file_bytes(path), payload)
        finally:
            if saved is None:
                del sys.modules["_io"]
            else:
                sys.modules["_io"] = saved
            try:
                os.remove(path)
            except OSError:
                pass

    # -- 完整 RPC 闭环与工厂 -----------------------------------------------------

    def test_full_rpc_roundtrip_through_file_bridge(self):
        """大QMT侧 RPC 服务配 FileRedisClient，miniQMT 侧直连 Redis 调 ping。"""
        from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService, call_redis_rpc

        account_id = "file-bridge-acc"
        handlers = BigQmtRpcHandlers(account_id=account_id, market_data=None, position_provider=None)
        service = RedisPubSubRpcService(
            redis_client=self.client,
            handlers=handlers,
            account_id=account_id,
            process_in_listener=True,
            listener_methods=("*",),
            background_threads=False,
        )
        service.start()
        stop = threading.Event()

        def pump():
            # 模拟 QMT 的 adjust 定时回调：周期性 drain 请求队列。
            while not stop.is_set():
                service.drain_request_queue(max_items=20)
                service.drain_pending(max_items=20)
                time.sleep(0.02)

        pumper = threading.Thread(target=pump, name="file-bridge-pump", daemon=True)
        pumper.start()
        try:
            response = call_redis_rpc(self.backend, account_id, "ping", timeout_seconds=5.0)
        finally:
            stop.set()
            pumper.join(2)
        self.assertTrue(response.get("ok"), response)
        self.assertTrue(response["data"]["pong"])

    def test_factory_file_dir_wins_over_legacy_relay_keys(self):
        # relay_* 键已随 TCP 中继删除而被忽略：配了 file_dir 就选 FileRedisClient。
        picked = build_redis_client({
            "file_dir": self.root_dir,
            "file_token": TOKEN,
            "relay_host": "127.0.0.1",
            "relay_port": 1,
            "relay_token": "nope",
        })
        try:
            self.assertIsInstance(picked, FileRedisClient)
            self.assertEqual(picked.file_dir, os.path.abspath(self.root_dir))
        finally:
            picked.close()

    def test_factory_ignores_legacy_relay_keys_without_file_dir(self):
        # 无 file_dir 且只有残留 relay_* 键：不再回落 TCP 中继（已删除），
        # 直接走真实 Redis（沙箱内该路径会被拦，但工厂行为如此）。
        picked = build_redis_client({
            "host": REDIS_HOST,
            "port": REDIS_PORT,
            "db": REDIS_DB,
            "relay_host": "127.0.0.1",
            "relay_port": 1,
        })
        try:
            self.assertIsInstance(picked, redis.Redis)
        finally:
            picked.close()

    def test_factory_defaults_to_real_redis_without_file_dir(self):
        direct = build_redis_client({"host": REDIS_HOST, "port": REDIS_PORT, "db": REDIS_DB})
        try:
            self.assertIsInstance(direct, redis.Redis)
            self.assertTrue(direct.ping())
        finally:
            direct.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

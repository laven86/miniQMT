# -*- coding: utf-8 -*-
"""qmt_relay_common 单元测试：白名单、token、key 前缀与编解码往返（零网络依赖）。

原 test_qmt_redis_relay.py 针对 TCP 中继服务器的端到端用例随中继删除
（2026-10-31）而废弃：沙箱封禁一切策略进程 TCP 外连，方案已证伪。
本文件只测抽到 qmt_relay_common 的公共校验/执行/编解码逻辑的安全面；
文件桥端到端（含完整 RPC 闭环与直连路径）由 test_qmt_file_relay.py 覆盖。
"""

import os
import sys
import unittest

import redis

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import qmt_relay_common as common

TOKEN = "test-token"
PREFIX = "bigqmt:"


class _StubRedis(object):
    """记录调用的假 Redis：只验证 execute 的分发与参数改写，不连网络。"""

    def __init__(self):
        self.calls = []

    def _record(self, cmd, *args, **kwargs):
        self.calls.append((cmd, args, kwargs))
        return "stub-" + cmd

    def __getattr__(self, name):
        def method(*args, **kwargs):
            return self._record(name, *args, **kwargs)

        return method


class ValidateRequestTest(unittest.TestCase):
    def _req(self, cmd, args, token=TOKEN):
        return {"id": 1, "token": token, "cmd": cmd, "args": args}

    def test_bad_token_is_rejected(self):
        with self.assertRaises(common.RelayError) as caught:
            common.validate_request(self._req("get", [PREFIX + "k"], token="nope"), TOKEN, PREFIX)
        self.assertIn("bad token", str(caught.exception))
        # 缺 token 同样拒绝。
        with self.assertRaises(common.RelayError):
            common.validate_request(self._req("get", [PREFIX + "k"], token=None), TOKEN, PREFIX)

    def test_empty_token_disables_auth(self):
        cmd, args = common.validate_request(self._req("get", [PREFIX + "k"], token="anything"), "", PREFIX)
        self.assertEqual((cmd, args), ("get", [PREFIX + "k"]))

    def test_disallowed_command_is_rejected(self):
        for cmd in ("flushdb", "config", "eval", "keys", "", "unknown_cmd"):
            with self.assertRaises(common.RelayError) as caught:
                common.validate_request(self._req(cmd, []), TOKEN, PREFIX)
            self.assertIn("not allowed", str(caught.exception))

    def test_command_is_case_insensitive(self):
        cmd, _ = common.validate_request(self._req("GET", [PREFIX + "k"]), TOKEN, PREFIX)
        self.assertEqual(cmd, "get")
        cmd, _ = common.validate_request(self._req("  Ping ", [], TOKEN), TOKEN, PREFIX)
        self.assertEqual(cmd, "ping")

    def test_key_outside_prefix_is_rejected(self):
        with self.assertRaises(common.RelayError) as caught:
            common.validate_request(self._req("get", ["other:key"]), TOKEN, PREFIX)
        self.assertIn("outside allowed prefix", str(caught.exception))
        # bytes key 同样校验（hgetall 返回 bytes 键的镜像场景）。
        with self.assertRaises(common.RelayError):
            common.validate_request(self._req("get", [b"other:key"]), TOKEN, PREFIX)

    def test_delete_checks_all_positional_args(self):
        # delete 的 key_args="all"：任一参数越界即拒绝。
        with self.assertRaises(common.RelayError):
            common.validate_request(
                self._req("delete", [PREFIX + "a", "FLUSHALL"]), TOKEN, PREFIX
            )
        cmd, args = common.validate_request(
            self._req("delete", [PREFIX + "a", PREFIX + "b"]), TOKEN, PREFIX
        )
        self.assertEqual(args, [PREFIX + "a", PREFIX + "b"])

    def test_empty_prefix_disables_key_check(self):
        cmd, _ = common.validate_request(self._req("get", ["anywhere"]), TOKEN, "")
        self.assertEqual(cmd, "get")

    def test_non_object_or_bad_args_rejected(self):
        with self.assertRaises(common.RelayError):
            common.validate_request(["not", "a", "dict"], TOKEN, PREFIX)
        # args 非列表：decode_value 对 dict 返回 dict，须被拒绝。
        with self.assertRaises(common.RelayError):
            common.validate_request(self._req("get", {"0": PREFIX + "k"}), TOKEN, PREFIX)

    def test_b64_args_are_decoded_before_key_check(self):
        # bytes key 经 __b64__ 传输：解码后照常校验前缀。
        import base64

        b64key = {"__b64__": base64.b64encode(b"other:key").decode("ascii")}
        with self.assertRaises(common.RelayError):
            common.validate_request(self._req("get", [b64key]), TOKEN, PREFIX)
        b64ok = {"__b64__": base64.b64encode((PREFIX + "k").encode()).decode("ascii")}
        cmd, args = common.validate_request(self._req("get", [b64ok]), TOKEN, PREFIX)
        self.assertEqual(args, [(PREFIX + "k").encode()])


class EncodeDecodeTest(unittest.TestCase):
    def test_bytes_roundtrip(self):
        payload = b'{"qty": 1}'
        encoded = common.encode_value(payload)
        self.assertEqual(encoded["__b64__"], "eyJxdHkiOiAxfQ==")
        self.assertEqual(common.decode_value(encoded), payload)
        # 非 ASCII bytes（含中文）同样往返保真。
        cjk = "中文负载".encode("utf-8")
        self.assertEqual(common.decode_value(common.encode_value(cjk)), cjk)

    def test_nested_structures_roundtrip(self):
        # list/bytes/标量多层嵌套往返保真；tuple 编码成 list（JSON 无 tuple）。
        value = [b"a", [1, None, True], ("t", b"c")]
        self.assertEqual(
            common.decode_value(common.encode_value(value)),
            [b"a", [1, None, True], ["t", b"c"]],
        )

    def test_dict_encodes_to_map_entries(self):
        # dict（含 bytes 键）编码成 {"__map__": [[k, v], ...]}；__map__ 是
        # 响应侧单向编码，decode_value 不还原——解包由策略端客户端
        # （file_redis_client）负责。这里只验证编码结构保真。
        value = {b"status": b"CLAIMED", b"qty": b"1"}
        encoded = common.encode_value(value)
        self.assertEqual(set(encoded.keys()), {"__map__"})
        pairs = encoded["__map__"]
        self.assertEqual(len(pairs), 2)
        self.assertEqual(
            sorted((common.decode_value(k), common.decode_value(v)) for k, v in pairs),
            sorted(value.items()),
        )

    def test_scalars_pass_through(self):
        for value in (None, True, False, 0, 1.5, "text"):
            self.assertEqual(common.decode_value(common.encode_value(value)), value)

    def test_bad_base64_rejected(self):
        with self.assertRaises(common.RelayError):
            common.decode_value({"__b64__": "!!!not base64!!!"})


class ExecuteTest(unittest.TestCase):
    def test_get_dispatches_to_client(self):
        stub = _StubRedis()
        result = common.execute(stub, "get", [PREFIX + "k"])
        self.assertEqual(result, "stub-get")
        self.assertEqual(stub.calls, [("get", (PREFIX + "k",), {})])

    def test_setnx_ex_maps_to_set_nx_ex(self):
        stub = _StubRedis()
        common.execute(stub, "setnx_ex", [PREFIX + "k", "consumer-a", 30])
        self.assertEqual(stub.calls, [("set", (PREFIX + "k", "consumer-a"), {"nx": True, "ex": 30})])

    def test_setex_maps_to_set_ex(self):
        stub = _StubRedis()
        common.execute(stub, "setex", [PREFIX + "k", 30, b"v"])
        self.assertEqual(stub.calls, [("set", (PREFIX + "k", b"v"), {"ex": 30})])

    def test_hset_mapping_call(self):
        stub = _StubRedis()
        common.execute(stub, "hset", [PREFIX + "k", {"status": "CLAIMED"}])
        self.assertEqual(stub.calls, [("hset", (PREFIX + "k",), {"mapping": {"status": "CLAIMED"}})])
        # 非 mapping（3 参 field/value 形式）走 getattr 分发。
        common.execute(stub, "hset", [PREFIX + "k", "field", "value"])
        self.assertEqual(stub.calls[-1], ("hset", (PREFIX + "k", "field", "value"), {}))


class BuildClientTest(unittest.TestCase):
    def test_build_redis_client_settings(self):
        client = common.build_redis_client("127.0.0.1", 6379, 5, "")
        self.assertIsInstance(client, redis.Redis)
        self.assertEqual(client.connection_pool.connection_kwargs["host"], "127.0.0.1")
        self.assertEqual(client.connection_pool.connection_kwargs["port"], 6379)
        self.assertEqual(client.connection_pool.connection_kwargs["db"], 5)
        self.assertIsNone(client.connection_pool.connection_kwargs["password"])
        # 密码为空串转 None，非空保留。
        secured = common.build_redis_client("127.0.0.1", 6379, 5, "secret")
        self.assertEqual(secured.connection_pool.connection_kwargs["password"], "secret")


if __name__ == "__main__":
    unittest.main(verbosity=2)

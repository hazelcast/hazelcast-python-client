import unittest

from hazelcast.config import Config
from hazelcast.db import _make_config, InterfaceError


class DbApiTest(unittest.TestCase):
    def test_make_config_invalid(self):
        test_cases = [
            ("Invalid dsn", Config(), {"dsn": "hz://"}),
            ("Both config and kwarg", Config(), {"user": "some-user"}),
            ("Both DSN and kwarg", None, {"dsn": "hz://", "password": "some-pass"}),
        ]
        for name, c, kwargs in test_cases:
            with self.assertRaises(InterfaceError, msg=f"Test case '{name}' failed"):
                _make_config(c, **kwargs)

    def test_make_config_default(self):
        cfg = _make_config()
        target = config_with_values(cluster_members=["localhost:5701"])
        self.assertEqualConfig(target, cfg)

    def test_make_config_config_object(self):
        cfg = config_with_values(creds_username="joe")
        target = config_with_values(creds_username="joe")
        self.assertEqualConfig(target, cfg)

    def test_make_config_cluster(self):
        cfg = _make_config(cluster_name="foo")
        target = config_with_values(
            cluster_members=["localhost:5701"],
            cluster_name="foo",
        )
        self.assertEqualConfig(target, cfg)

    def test_make_config_user_password(self):
        cfg = _make_config(user="joe", password="jane")
        target = config_with_values(
            cluster_members=["localhost:5701"],
            creds_username="joe",
            creds_password="jane",
        )
        self.assertEqualConfig(target, cfg)

    def test_make_config_host(self):
        cfg = _make_config(host="foo.com")
        target = config_with_values(cluster_members=["foo.com:5701"])
        self.assertEqualConfig(target, cfg)

    def test_make_config_port(self):
        cfg = _make_config(port=1234)
        target = config_with_values(cluster_members=["localhost:1234"])
        self.assertEqualConfig(target, cfg)

    def test_make_config_host_port(self):
        cfg = _make_config(host="foo.com", port=1234)
        target = config_with_values(cluster_members=["foo.com:1234"])
        self.assertEqualConfig(target, cfg)

    def test_make_config_dsn(self):
        test_cases = [
            ("hz://", config_with_values(cluster_members=["localhost:5701"])),
            ("hz://foo.com", config_with_values(cluster_members=["foo.com:5701"])),
            ("hz://:1234", config_with_values(cluster_members=["localhost:1234"])),
            ("hz://foo.com:1234", config_with_values(cluster_members=["foo.com:1234"])),
            (
                "hz://user:pass@foo.com:1234",
                config_with_values(
                    cluster_members=["foo.com:1234"],
                    creds_username="user",
                    creds_password="pass",
                ),
            ),
            (
                "hz://foo.com:1234?cluster.name=prod",
                config_with_values(
                    cluster_members=["foo.com:1234"],
                    cluster_name="prod",
                ),
            ),
            (
                "hz://foo.com:1234?cluster.name=prod&cloud.token=token1",
                config_with_values(
                    cluster_members=["foo.com:1234"],
                    cluster_name="prod",
                    cloud_discovery_token="token1",
                ),
            ),
            (
                "hz://foo.com?smart=false",
                config_with_values(
                    cluster_members=["foo.com:5701"],
                    smart_routing=False,
                ),
            ),
            (
                "hz://foo.com?ssl=true&ssl.ca.path=ca.pem&ssl.cert.path=cert.pem&ssl.key.path=key.pem&ssl.key.password=123",
                config_with_values(
                    cluster_members=["foo.com:5701"],
                    ssl_enabled=True,
                    ssl_cafile="ca.pem",
                    ssl_certfile="cert.pem",
                    ssl_keyfile="key.pem",
                    ssl_password="123",
                ),
            ),
        ]
        for dsn, target in test_cases:
            cfg = _make_config(dsn=dsn)
            self.assertEqualConfig(target, cfg, f"Test case with DSN '{dsn}' failed")

    def test_make_config_invalid_dsn(self):
        test_cases = [
            "http://",
            "://",
            "hz://foo.com?smart=False",
            "hz://foo.com?non.existing=value",
        ]
        for dsn in test_cases:
            with self.assertRaises(InterfaceError, msg=f"Test case with DSN '{dsn}' failed"):
                _make_config(dsn=dsn)

    def assertEqualConfig(self, a: Config, b: Config, msg=""):
        self.assertEqual(config_to_dict(a), config_to_dict(b), msg)


def config_with_values(**kwargs) -> Config:
    return Config.from_dict(kwargs)


def config_to_dict(cfg: Config) -> dict:
    d = {}
    for k in cfg.__slots__:
        d[k] = getattr(cfg, k)
    return d

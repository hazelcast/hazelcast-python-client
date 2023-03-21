import unittest

from hazelcast.config import Config
from hazelcast.db import make_config


class DbApiTestCase(unittest.TestCase):
    def test_make_config_default(self):
        cfg = make_config()
        target = config_with_values(cluster_members=["localhost:5701"])
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_config_object(self):
        cfg = config_with_values(creds_username="joe")
        target = config_with_values(creds_username="joe")
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_cluster(self):
        cfg = make_config(cluster="foo")
        target = config_with_values(
            cluster_members=["localhost:5701"],
            cluster_name="foo",
        )
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_user_password(self):
        cfg = make_config(user="joe", password="jane")
        target = config_with_values(
            cluster_members=["localhost:5701"],
            creds_username="joe",
            creds_password="jane",
        )
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_host(self):
        cfg = make_config(host="foo.com")
        target = config_with_values(cluster_members=["foo.com:5701"])
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_port(self):
        cfg = make_config(port=1234)
        target = config_with_values(cluster_members=["localhost:1234"])
        self.assertEqual(target.__dict__, cfg.__dict__)

    def test_make_config_host_port(self):
        cfg = make_config(host="foo.com", port=1234)
        target = config_with_values(cluster_members=["foo.com:1234"])
        self.assertEqual(target.__dict__, cfg.__dict__)


def config_with_values(**kwargs) -> Config:
    cfg = Config()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg

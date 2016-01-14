from hazelcast.proxy.base import Proxy


class ReliableTopic(Proxy):
    def add_listener(self, on_message=None):
        raise NotImplementedError

    def publish(self, message):
        raise NotImplementedError

    def remove_listener(self, registration_id):
        raise NotImplementedError

    def __str__(self):
        return "ReliableTopic(name=%s)" % self.name

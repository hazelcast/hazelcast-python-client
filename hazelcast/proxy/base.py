class Proxy(object):
    def __init__(self, client, service_name, name):
        self.service_name = service_name
        self.name = name
        self._client = client

    def destroy(self):
        pass
        # TODO

    def __str__(self):
        return '%s(name="%s")' % (type(self), self.name)

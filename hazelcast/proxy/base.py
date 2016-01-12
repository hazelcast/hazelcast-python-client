class Proxy(object):
    def __init__(self, client, service_name, name):
        self.service_name = service_name
        self.name = name
        self._client = client

    def destroy(self):
        return self._client.proxy.destroy_proxy(self.service_name, self.name)

    def __str__(self):
        return '%s(name="%s")' % (type(self), self.name)

    def _to_data(self, val):
        return self._client.serializer.to_data(val)

    def _to_object(self, data):
        return self._client.serializer.to_object(data)

    def _invoke(self, request):
        return self._client.invoker.invoke_on_random_target(request).result()

    def _invoke_on_key(self, request, key_data):
        partition_id = self._client.partition_service.get_partition_id(key_data)
        return self._client.invoker.invoke_on_partition(request, partition_id).result()

    def _invoke_on_key_async(self, request, key_data):
        partition_id = self._client.partition_service.get_partition_id(key_data)
        return self._client.invoker.invoke_on_partition(request, partition_id)

    def _invoke_on_partition(self, request, partition_id):
        return self._client.invoker.invoke_on_partition(request, partition_id).result()

    def _start_listening(self, request, event_handler, response_decoder, key=None):
        return self._client.listener.start_listening(request, event_handler, response_decoder, key)

    def _stop_listening(self, registration_id, request_encoder):
        return self._client.listener.stop_listening(registration_id, request_encoder)

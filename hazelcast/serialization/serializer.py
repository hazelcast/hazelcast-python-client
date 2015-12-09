class BaseSerializerAdaptor(object):
    def write(self, out, obj):
        pass

    def read(self, inp):
        pass

    def get_type_id(self):
        pass

    def get_implementation(self):
        pass

    def destroy(self):
        pass


class StreamSerializerAdapter(BaseSerializerAdaptor):
    def __init__(self, serializer, service):
        self._service = service
        self._serializer = serializer

    def write(self, out, obj):
        self._serializer.write(out, obj)

    def read(self, inp):
        return self._serializer.read(inp)

    def get_type_id(self):
        return self._serializer.get_type_id

    def get_implementation(self):
        return self._serializer

    def destroy(self):
        self._serializer.destroy()

    def __hash__(self):
        return 0 if self._serializer is None else self._serializer.__hash__()

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or self.__class__ != other.__class__:
            return False
        if self._serializer is not None:
            return self._serializer == other._serializer
        else:
            return other._serializer is None


class ByteArraySerializerAdaptor(BaseSerializerAdaptor):
    def __init__(self, serializer, service):
        self._service = service
        self._serializer = serializer

    def read(self, inp):
        buff = inp.read_byte_array()
        if buff is None:
            return None
        return self._serializer.read(buff)

    def write(self, out, obj):
        buff = self._serializer.write(obj)
        out.write_byte_array(buff)

    def get_type_id(self):
        return self._serializer.get_type_id

    def get_implementation(self):
        return self._serializer

    def destroy(self):
        self._serializer.destroy()

    def __hash__(self):
        return 0 if self._serializer is None else self._serializer.__hash__()

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or self.__class__ != other.__class__:
            return False
        if self._serializer is not None:
            return self._serializer == other._serializer
        else:
            return other._serializer is None


class StreamSerializer(object):
    def write(self, out, obj):
        pass

    def read(self, inp):
        return None


class ByteArraySerializer(object):
    def write(self, obj):
        pass

    def read(self, buff):
        return None

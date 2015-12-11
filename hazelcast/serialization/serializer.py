class StreamSerializer(object):
    def write(self, out, obj):
        pass

    def read(self, inp):
        return None

    def get_type_id(self):
        pass

    def destroy(self):
        pass


class BufferSerializer(object):
    def write(self, obj):
        pass

    def read(self, buff):
        return None

    def get_type_id(self):
        pass

    def destroy(self):
        pass


class BufferSerializerWrapper(StreamSerializer):
    def __init__(self, buffer_serializer):
        self.buffer_serializer = buffer_serializer

    def read(self, inp):
        buff = inp.read_byte_array()
        if buff is None:
            return None
        return self.buffer_serializer.read(buff)

    def write(self, out, obj):
        buff = self.buffer_serializer.write(obj)
        out.write_byte_array(buff)

    def get_type_id(self):
        return self.buffer_serializer.get_type_id

    def destroy(self):
        self.buffer_serializer.destroy()

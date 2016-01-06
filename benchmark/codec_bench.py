import timeit

from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec import map_get_codec
from hazelcast.serialization import SerializationServiceV1, calculate_size_data


class Bench(object):
    def __init__(self):
        service = SerializationServiceV1(None)
        key = "Test" * 1000
        self.name = "name" * 10
        self.key = service.to_data(key)
        self.thread_id = 1l
        msg = ClientMessage(payload_size=calculate_size_data(self.key)).append_bool(False).append_data(
            self.key).update_frame_length()
        self.response_message = ClientMessage(msg.buffer)

        self.request = None
        self.response = None

    def encode(self):
        self.request = map_get_codec.encode_request(self.name, self.key, self.thread_id)

    def decode(self):
        self.response_message._read_index = 0
        self.response = map_get_codec.decode_response(self.response_message)

    def measure(self):
        print "Encode time: {}".format(timeit.timeit(self.encode, number=100000))
        # print "Decode time: {}".format(timeit.timeit(self.decode, number=100000))


if __name__ == '__main__':
    global bench
    bench = Bench()

    setup = "from __main__ import Bench"
    # setup = "from __main__ import Bench;global bench;bench = Bench()"
    number = 100000
    encode_time = timeit.timeit(bench.encode, setup=setup, number=number)
    decode_time = timeit.timeit(bench.decode, setup=setup, number=number)

    print "--------------------------------------------------------------------------------"
    print "Encode op/s: {}".format(number / encode_time)
    print "Decode op/s: {}".format(number / decode_time)
    print "Total  op/s: {}".format(number / (encode_time + decode_time))
    print "--------------------------------------------------------------------------------\n\n"
    print bench.request
    print bench.response

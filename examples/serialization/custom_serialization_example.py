import hazelcast
import logging

from hazelcast.serialization.api import StreamSerializer


class TimeOfDay(object):
    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second


class CustomSerializer(StreamSerializer):
    CUSTOM_SERIALIZER_ID = 4  # Should be greater than 0 and unique to each serializer

    def read(self, inp):
        seconds = inp.read_int()
        second = seconds % 60
        seconds = (seconds - second) // 60
        minute = seconds % 60
        seconds = (seconds - minute) // 60
        hour = seconds
        return TimeOfDay(hour, minute, second)

    def write(self, out, obj):
        seconds = obj.hour * 3600 + obj.minute * 60 + obj.second
        out.write_int(seconds)

    def get_type_id(self):
        return self.CUSTOM_SERIALIZER_ID

    def destroy(self):
        pass


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()
    config.serialization_config.set_custom_serializer(type(TimeOfDay), CustomSerializer)

    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map")
    time_of_day = TimeOfDay(13, 36, 59)
    my_map.put("time", time_of_day)

    time = my_map.get("time").result()
    print("Time is {}:{}:{}".format(time.hour, time.minute, time.second))

    client.shutdown()

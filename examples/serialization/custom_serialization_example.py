import logging
import hazelcast

from hazelcast.serialization.api import StreamSerializer

logging.basicConfig(level=logging.INFO)


class TimeOfDay:
    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second

    def __repr__(self):
        return f"TimeOfDay(hour={self.hour}, minute={self.minute}, second={self.second})"


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


client = hazelcast.HazelcastClient(
    custom_serializers={
        TimeOfDay: CustomSerializer,
    },
)

time_map = client.get_map("time_map").blocking()

time_of_day = TimeOfDay(13, 36, 59)
time_map.put("time", time_of_day)

time = time_map.get("time")
print("Time is", time)

client.shutdown()

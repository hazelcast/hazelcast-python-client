import hazelcast
from hazelcast.core import HazelcastJsonValue


if __name__ == "__main__":
    json_client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )

    id_person_map = json_client.get_map("my-json-map").blocking()

    person1 = '{ "name": "John", "age": 35 }'
    person2 = '{ "name": "Jane", "age": 24 }'
    person3 = '{ "name": "Trey", "age": 17 }'

    id_person_map.put(1, HazelcastJsonValue(person1))
    id_person_map.put(2, HazelcastJsonValue(person2))
    id_person_map.put(3, HazelcastJsonValue(person3))

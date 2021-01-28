import hazelcast

from hazelcast.serialization.api import IdentifiedDataSerializable


class Student(IdentifiedDataSerializable):
    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, id=None, name=None, gpa=None):
        self.id = id
        self.name = name
        self.gpa = gpa

    def read_data(self, object_data_input):
        self.id = object_data_input.read_int()
        self.name = object_data_input.read_string()
        self.gpa = object_data_input.read_float()

    def write_data(self, object_data_output):
        object_data_output.write_int(self.id)
        object_data_output.write_string(self.name)
        object_data_output.write_float(self.gpa)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return "Student(id=%s, name=%s, gpa=%s)" % (self.id, self.name, self.gpa)


client = hazelcast.HazelcastClient(
    data_serializable_factories={Student.FACTORY_ID: {Student.CLASS_ID: Student}}
)

my_map = client.get_map("map")

student = Student(1, "John Doe", 3.0)

my_map.put("student1", student)

returned_student = my_map.get("student1").result()

print("Received:", returned_student)

client.shutdown()

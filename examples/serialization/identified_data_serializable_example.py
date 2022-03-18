import logging
import hazelcast

from hazelcast.serialization.api import IdentifiedDataSerializable

logging.basicConfig(level=logging.INFO)


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
        return f"Student(id={self.id}, name={self.name}, gpa={self.gpa})"


client = hazelcast.HazelcastClient(
    data_serializable_factories={
        Student.FACTORY_ID: {
            Student.CLASS_ID: Student,
        },
    },
)

students = client.get_map("students").blocking()

student = Student(1, "John Doe", 3.0)

students.put("student1", student)

returned_student = students.get("student1")
print(f"Received: {returned_student}")

client.shutdown()

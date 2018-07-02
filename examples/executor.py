import hazelcast
import logging
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast import six


#     ####### Server side code #######
#
# class CollectData implements IdentifiedDataSerializable, Callable<String> {
#    public static int CLASS_ID = 12;
#    private String propertyKey;
#
#    CollectData() {
#
#    }
#
#    CollectData(String propertyKey) {
#        this.propertyKey = propertyKey;
#    }
#
#    public int getFactoryId() {
#        return 3;
#    }
#
#    public int getId() {
#        return 12;
#    }
#
#    public void writeData(ObjectDataOutput out) throws IOException {
#        out.writeUTF(propertyKey);
#    }
#
#    public void readData(ObjectDataInput in) throws IOException {
#        propertyKey = in.readUTF();
#    }
#
#    public String call() throws Exception {x
#        return System.getProperty(propertyKey);
#    }
# }
#
# public class XServer {
#
#    public static void main(String[] args) {
#        Config config = new Config();
#        SerializationConfig serializationConfig = config.getSerializationConfig();
#        serializationConfig.addDataSerializableFactory(3, new DataSerializableFactory() {
#            public IdentifiedDataSerializable create(int typeId) {
#                if (CollectData.CLASS_ID == typeId) {
#                    return new CollectData();
#                }
#                return null;
#            }
#        });
#        Hazelcast.newHazelcastInstance(config);
#
#    }
# }

class CollectData(IdentifiedDataSerializable):
    CLASS_ID = 12
    FACTORY_ID = 3

    def __init__(self, propertyKey):
        self.propertyKey = propertyKey

    def write_data(self, out):
        out.write_utf(self.propertyKey)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"
    config.network_config.addresses.append('127.0.0.1')

    identifiedDataSerializable_factory = {CollectData.CLASS_ID: CollectData}
    config.serialization_config.add_data_serializable_factory(CollectData.FACTORY_ID, identifiedDataSerializable_factory)

    client = hazelcast.HazelcastClient(config)
    executor = client.get_executor("my-exec")
    results = executor.execute_on_all_members(CollectData("java.vm.name"))
    for result in results.result():
        six.print_(">", result)

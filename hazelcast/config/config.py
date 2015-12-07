class ClientConfig(object):
    def __init__(self):
        self.properties = {}
        self.groupConfig = GroupConfig()
        # self.securityConfig = ClientSecurityConfig()
        self.networkConfig = ClientNetworkConfig()
        # self.loadBalancer = LoadBalancer()
        self.listenerConfigs = []
        self.executorPoolSize = -1
        self.instanceName = None
        self.nearCacheConfigs = {}
        self.reliableTopicConfigs = {}
        self.queryCacheConfigs = {}
        self.serializationConfig = SerializationConfig()
        self.proxyFactoryConfigs = {}


DEFAULT_GROUP_NAME = "dev"
DEFAULT_GROUP_PASSWORD = "dev-pass"


class GroupConfig(object):
    def __init__(self):
        self.name = DEFAULT_GROUP_NAME
        self.password = DEFAULT_GROUP_PASSWORD


class ClientNetworkConfig(object):
    def __init__(self):
        self.addressList = []
        self.connectionAttemptLimit = 2
        self.connectionAttemptPeriod = 3000
        self.connectionTimeout = 5000
        self.redoOperation = False
        self.smartRouting = True
        # self.socketInterceptorConfig = SocketInterceptorConfig()
        # self.socketOptions = new SocketOptions()


class SocketOptions(object):
    def __init__(self):
        super(SocketOptions, self).__init__()


class SerializationConfig(object):
    def __init__(self):
        self.portableVersion = 0
        self.dataSerializableFactoryClasses = {}
        self.dataSerializableFactories = {}
        self.portableFactoryClasses = {}
        self.portableFactories = {}
        self.globalSerializerConfig = None
        self.serializerConfigs = []
        self.checkClassDefErrors = True
        self.useNativeByteOrder = False
        # self.byteOrder = BIG_ENDIAN
        self.enableCompression = False
        self.enableSharedObject = True
        self.classDefinitions = set()


class GlobalSerializerConfig(object):
    def __init__(self):
        pass

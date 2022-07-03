import logging

from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from tests.hzrc import RemoteController


class HzRemoteController(RemoteController.Iface):
    logger = logging.getLogger("HzRemoteController")

    def __init__(self, host, port):
        try:
            # Make socket
            transport = TSocket.TSocket(host=host, port=port)
            # Buffering is critical. Raw sockets are very slow
            transport = TTransport.TBufferedTransport(transport)
            # Wrap in a protocol
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self.remote_controller = RemoteController.Client(protocol)
            # Connect!
            transport.open()
        except Thrift.TException:
            self.logger.exception("Something went wrong while connecting to remote controller.")

    def ping(self):
        return self.remote_controller.ping()

    def clean(self):
        return self.remote_controller.clean()

    def exit(self):
        self.remote_controller.exit()
        self.remote_controller._iprot.trans.close()

    def createCluster(self, hz_version, xml_config):
        return self.remote_controller.createCluster(hz_version, xml_config)

    def createClusterKeepClusterName(self, hz_version, xml_config):
        return self.remote_controller.createClusterKeepClusterName(hz_version, xml_config)

    def startMember(self, cluster_id):
        return self.remote_controller.startMember(cluster_id)

    def shutdownMember(self, cluster_id, member_id):
        return self.remote_controller.shutdownMember(cluster_id, member_id)

    def terminateMember(self, cluster_id, member_id):
        return self.remote_controller.terminateMember(cluster_id, member_id)

    def suspendMember(self, cluster_id, member_id):
        return self.remote_controller.suspendMember(cluster_id, member_id)

    def resumeMember(self, cluster_id, member_id):
        return self.remote_controller.resumeMember(cluster_id, member_id)

    def shutdownCluster(self, cluster_id):
        return self.remote_controller.shutdownCluster(cluster_id)

    def terminateCluster(self, cluster_id):
        return self.remote_controller.terminateCluster(cluster_id)

    def splitMemberFromCluster(self, member_id):
        return self.remote_controller.splitMemberFromCluster(member_id)

    def mergeMemberToCluster(self, cluster_id, member_id):
        return self.remote_controller.mergeMemberToCluster(cluster_id, member_id)

    def executeOnController(self, cluster_id, script, lang):
        return self.remote_controller.executeOnController(cluster_id, script, lang)

    # Cloud APIs

    def loginToHazelcastCloudUsingEnvironment(self):
        return self.remote_controller.loginToHazelcastCloudUsingEnvironment()

    def loginToHazelcastCloud(self, uri, api_key, api_secret):
        return self.remote_controller.loginToHazelcastCloud(uri, api_key, api_secret)

    def createHazelcastCloudStandardCluster(self, hz_version, is_tls_enabled):
        return self.remote_controller.createHazelcastCloudStandardCluster(hz_version, is_tls_enabled)

    def getHazelcastCloudCluster(self, cloud_cluster_id):
        return self.remote_controller.getHazelcastCloudCluster(cloud_cluster_id)

    def setHazelcastCloudClusterMemberCount(self, cloud_cluster_id, total_member_count):
        return self.remote_controller.setHazelcastCloudClusterMemberCount(cloud_cluster_id, total_member_count)

    def stopHazelcastCloudCluster(self, cloud_cluster_id):
        return self.remote_controller.stopHazelcastCloudCluster(cloud_cluster_id)

    def resumeHazelcastCloudCluster(self, cloud_cluster_id):
        return self.remote_controller.resumeHazelcastCloudCluster(cloud_cluster_id)

    def deleteHazelcastCloudCluster(self, cloud_cluster_id):
        return self.remote_controller.deleteHazelcastCloudCluster(cloud_cluster_id)


import hazelcast

if __name__ == '__main__':

    # import hazelcast.codec.client_authentication_codec
    #
    # b = hazelcast.codec.client_authentication_codec.encode_request("dev", "dev-pass2", "uuid", "owner-uuid", False,
    #                                                                "Python", 1)
    # import binascii
    # print(binascii.hexlify(b.to_bytes()))
    #

    config = hazelcast.Config()
    config.username = "dev"
    config.password = "dev-pass"
    config.add_address("127.0.0.1:5701")

    client = hazelcast.HazelcastClient(config)
    #map = client.get_map("map")
    #map.put("key", "value")
    # print(map.get("key"))

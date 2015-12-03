from hazelcast.connection import Address

def decode_address(parser):
    return Address(parser.read_str(), parser.read_int())

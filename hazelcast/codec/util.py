class Member(object):
    def __init__(self, address, uuid, is_lite_member, attributes):
        self.address = address
        self.uuid = uuid
        self.is_lite_member = is_lite_member
        self.attributes = attributes

    def __str__(self):
        return str(self.address)

    def __repr__(self):
        return repr(self.address)

def decode_address(message):
    return message.read_str(), message.read_int()

def decode_member(message):
    address = decode_address(message)
    uuid = message.read_str()
    is_lite_member = message.read_bool()
    attribute_size = message.read_int()
    attributes = {message.read_str(): message.read_str() for _ in xrange(0, attribute_size)}
    return Member(address, uuid, is_lite_member, attributes)

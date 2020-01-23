#fixed size types codec encode_int
from hazelcast.protocol.client_message import ClientMessage


class StringCodec:
	@staticmethod
	def encode(client_message, value):
		if not type(value) == type("str"):
			client_message.add(ClientMessage.Frame(bytearray(value.name,"utf-8")))
		else:
			client_message.add(ClientMessage.Frame(bytearray(value, "utf-8")))

	@staticmethod
	def decode(iterator_or_frame):
		if type(iterator_or_frame) == type(ClientMessage.Frame(bytearray())):
			return iterator_or_frame.content.decode("utf-8")
		else:
			return StringCodec.decode(iterator_or_frame.next())

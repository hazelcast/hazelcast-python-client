from hazelcast.protocol.client_message import ClientMessage


def encode(client_message, value):
    if not isinstance(value, str):
        client_message.add(ClientMessage.Frame(bytearray(value.name, "utf-8")))
    else:
        client_message.add(ClientMessage.Frame(bytearray(value, "utf-8")))


def decode(iterator_or_frame):
    if isinstance(iterator_or_frame, ClientMessage.Frame):
        return iterator_or_frame.content.decode("utf-8")
    else:
        return decode(iterator_or_frame.next())

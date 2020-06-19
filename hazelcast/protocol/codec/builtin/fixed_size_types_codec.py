import hazelcast.protocol.bits as Bits
import struct
import uuid

BYTE_SIZE_IN_BYTES = Bits.BYTE_SIZE_IN_BYTES
LONG_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES
INT_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES
ENUM_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES
BOOLEAN_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES
UUID_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES * 2


def encode_int(buffer, pos, value):
	struct.pack_into(Bits.FMT_LE_INT, buffer, pos, value)


def decode_int(buffer, pos):
	return struct.unpack_from(Bits.FMT_LE_INT, buffer, pos)[0]


def decode_enum(buffer, pos):
	return decode_int(buffer, pos)


def encode_long(buffer, pos, value):
	struct.pack_into(Bits.FMT_LE_LONG, buffer, pos, value)


def decode_long(buffer, pos):
	return struct.unpack_from(Bits.FMT_LE_LONG, buffer, pos)[0]


def decode_long_unsigned(buffer, pos):
	return struct.unpack_from(Bits.FMT_LE_ULONG, buffer, pos)[0]


def encode_uuid(buffer, pos, value):
	is_null = value is None
	encode_boolean(buffer, pos, is_null)
	if is_null:
		return

	msb, lsb = struct.unpack(">qq", value.bytes)
	encode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES, msb)
	encode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES, lsb)


def decode_uuid(buffer, pos):
	is_null = decode_boolean(buffer, pos)
	if is_null:
		return None
	msb = decode_long_unsigned(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES)
	lsb = decode_long_unsigned(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES)
	return uuid.UUID(int=(lsb & 0xffffffffffffffff) | (msb << 64))


def encode_boolean(buffer, pos, value):
	buffer[pos] = 1 if value else 0


def decode_boolean(buffer, pos):
	return buffer[pos] == 1


def encode_byte(buffer, pos, value):
	buffer[pos] = value


def decode_byte(buffer, pos):
	return buffer[pos]

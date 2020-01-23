import hazelcast.protocol.bits as Bits
import struct
import uuid

BYTE_SIZE_IN_BYTES = Bits.BYTE_SIZE_IN_BYTES
LONG_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES
INT_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES
ENUM_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES
BOOLEAN_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES
UUID_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES * 2


class FixedSizeTypesCodec:

	@staticmethod
	def encode_int(buffer, pos, value):
		struct.pack_into(Bits.FMT_LE_INT, buffer, pos, value)

	@staticmethod
	def decode_int(buffer, pos):
		struct.unpack_from(Bits.FMT_LE_INT, buffer, pos)

	@staticmethod
	def decode_enum(buffer, pos):
		return FixedSizeTypesCodec.decode_int(buffer, pos)

	@staticmethod
	def encode_long(buffer, pos, value):
		struct.pack_into(Bits.FMT_LE_LONG, buffer, pos, value)

	@staticmethod
	def decode_long(buffer, pos):
		return struct.unpack_from(Bits.FMT_LE_LONG, buffer, pos)[0]

	@staticmethod
	def encode_uuid(buffer, pos, value):
		is_null = value is None
		FixedSizeTypesCodec.encode_boolean(buffer, pos, is_null)
		if is_null:
			return
		#msb = (value.int >> 64) & 0xFFFFFFFFFFFFFFFF	#2581521426421336153
		#lsb = (value.int << 64 & 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) >> 64	#-7846814559864137868
		msb, lsb = struct.unpack(">qq", value.bytes)
		FixedSizeTypesCodec.encode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES, msb)
		FixedSizeTypesCodec.encode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES, lsb)

	@staticmethod
	def decode_uuid(buffer, pos):
		is_null = FixedSizeTypesCodec.decode_boolean(buffer, pos)
		if is_null:
			return None
		msb = FixedSizeTypesCodec.decode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES)
		lsb = FixedSizeTypesCodec.decode_long(buffer, pos + Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES)
		return uuid.UUID(int=(msb << 64 + lsb))

	@staticmethod
	def encode_boolean(buffer, pos, value):
		buffer[pos] = 1 if value else 0

	@staticmethod
	def decode_boolean(buffer, pos):
		return buffer[pos] == 1

	@staticmethod
	def encode_byte(buffer, pos, value):
		buffer[pos] = value

	@staticmethod
	def decode_byte(buffer, pos):
		return buffer[pos]


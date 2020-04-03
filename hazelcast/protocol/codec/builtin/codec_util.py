from hazelcast.protocol.client_message import NULL_FRAME


class CodecUtil:
    @staticmethod
    def fast_forward_to_end_frame(iterator):
        number_of_expected_end_frames = 1
        while number_of_expected_end_frames != 0:
            frame = iterator.next()
            if frame.is_end_frame():
                number_of_expected_end_frames -= number_of_expected_end_frames
            elif frame.is_begin_frame():
                number_of_expected_end_frames += number_of_expected_end_frames

    @staticmethod
    def encode_nullable(client_message, value, encode):
        if value is None:
            client_message.add(NULL_FRAME.copy())
        else:
            encode(client_message, value)

    @staticmethod
    def decode_nullable(iterator, decode):
        if CodecUtil.next_frame_is_null_end_frame(iterator):
            return None
        else:
            return decode(iterator)

    @staticmethod
    def next_frame_is_data_structure_end_frame(iterator):
        return iterator.peek_next().is_end_frame()

    @staticmethod
    def next_frame_is_null_end_frame(iterator):
        is_null = iterator.peek_next().is_null_frame()
        if is_null:
            iterator.next()
        return is_null

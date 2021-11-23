import zlib

from hazelcast.serialization import (
    BYTE_SIZE_IN_BYTES,
    CHAR_SIZE_IN_BYTES,
    BE_UINT16,
    DOUBLE_SIZE_IN_BYTES,
    BE_DOUBLE,
    INT_SIZE_IN_BYTES,
    BE_INT,
    LONG_SIZE_IN_BYTES,
    BE_LONG,
    BE_UINT8,
)

_MASK_PREFIX = 1
_MASK_METRIC = 1 << 1
_MASK_DISCRIMINATOR = 1 << 2
_MASK_DISCRIMINATOR_VALUE = 1 << 3
_MASK_UNIT = 1 << 4
_MASK_EXCLUDED_TARGETS = 1 << 5
_MASK_TAG_COUNT = 1 << 6

_NULL_DICTIONARY_ID = -1
_NULL_UNIT = -1
_MAX_WORD_LENGTH = 255

_BITS_IN_BYTE = 8
_BYTE_MASK = 0xFF
_BINARY_FORMAT_VERSION = 1
_SIZE_VERSION = 2
_SIZE_DICTIONARY_BLOB = 4
_SIZE_COUNT_METRICS = 4

_OUTPUT_BUFFER_INITIAL_SIZE = 1024
_OUTPUT_BUFFER_GROW_FACTOR = 1.2


class MetricDescriptor(object):
    """Describes a metric to be sent to the members.

    It is a simplified version of the Java's MetricDescriptorImpl, sufficient
    for the needs of the Python client.
    """

    __slots__ = ("prefix", "metric", "discriminator", "discriminator_value", "unit")

    def __init__(
        self, metric, prefix=None, discriminator=None, discriminator_value=None, unit=None
    ):
        self.metric = metric
        """str: Name of the metric."""

        self.prefix = prefix
        """str: Prefix of the metric."""

        self.discriminator = discriminator
        """str: Discriminator for the metrics that have the same name and prefix."""

        self.discriminator_value = discriminator_value
        """str: Tag of the discriminator."""

        self.unit = unit
        """ProbeUnit: Unit of the metric."""


class ProbeUnit(object):
    """Measurement unit of a probe.

    The values of the constants below should be in sync with the ProbeUnit
    enum in Java.
    """

    BYTES = 0
    """Size or counter represented in bytes."""

    MS = 1
    """Timestamp or duration represented in milliseconds."""

    NS = 2
    """Timestamp or duration represented in nanoseconds."""

    PERCENT = 3
    """An integer mostly in range 0..100 or a double mostly in range 0..1."""

    COUNT = 4
    """Number of items: size, counter..."""

    BOOLEAN = 5
    """0 or 1."""

    ENUM = 6
    """0..n, ordinal of an enum."""

    # New enum values should be converted to a tag to ensure backward
    # compatibility during compressing the metrics. see ProbeUnit#newUnit
    # handling in Java for that. We don't implement this functionality yet in
    # the Python client, as we don't use such enum members.


class ValueType(object):
    """Type of the metric values.

    The values of the constants below should be in sync ValueType enum in Java.
    """

    LONG = 0
    DOUBLE = 1


class MetricsCompressor(object):
    """Compresses metrics into a ``bytearray`` blob.

    The compressor uses dictionary based delta compression and deflates
    the resulting ``bytearray`` by using ``zlib.compress()``. This
    compressor doesn't use the textual representation of the
    :class:`MetricDescriptor` hence it is agnostic to the order of the
    tags in that representation.

    Adding metrics by calling :func:`add_long` or :func:`add_double`
    builds a dictionary by mapping all words found in the passed
    :class:`MetricDescriptor`s to ``int``s and these ``int``s will be
    written to the resulting ``bytearray`` blob. Before these ``int``s
    are written, the current :class:`MetricDescriptor` is compared to the
    previous one and only the fields of the descriptor that are different
    from the previous will be written to the metrics blob.

    When the blob is retrieved from this compressor (when the metrics
    collection cycle finishes) the dictionary is stored in the dictionary
    blob. The compressor iterates over the words stored in the dictionary
    in ascending order and writes them to the blob by skipping the first
    N characters that are equal to the first N character of the previously
    written word, hence using delta compression here too.

    After both the metrics and the dictionary blob is constructed, they
    are copied into a final blob in the following structure:

    +--------------------------------+--------------------+
    | Compressor version             |   2 bytes (short)  |
    +--------------------------------+--------------------+
    | Size of dictionary blob        |   4 bytes (int)    |
    +--------------------------------+--------------------+
    | Dictionary blob                |   variable size    |
    +--------------------------------+--------------------+
    | Number of metrics in the blob  |   4 bytes (int)    |
    +--------------------------------+--------------------+
    | Metrics blob                   |   variable size    |
    +--------------------------------+--------------------+
    """

    __slots__ = ("_metrics_buf", "_dict_buf", "_metrics_dict", "_metrics_count", "_last_descriptor")

    def __init__(self):
        self._metrics_buf = _OutputBuffer()
        self._dict_buf = _OutputBuffer()
        self._metrics_dict = _MetricsDictionary()
        self._metrics_count = 0
        self._last_descriptor = None

    def add_long(self, descriptor, value):
        self._write_descriptor(descriptor)
        self._metrics_buf.write_byte(ValueType.LONG)
        self._metrics_buf.write_long(value)

    def add_double(self, descriptor, value):
        self._write_descriptor(descriptor)
        self._metrics_buf.write_byte(ValueType.DOUBLE)
        self._metrics_buf.write_double(value)

    def generate_blob(self):
        self._write_metrics_dict()
        metrics_buf = self._metrics_buf.compress()
        dict_buf = self._dict_buf.compress()

        complete_size = (
            _SIZE_VERSION
            + _SIZE_DICTIONARY_BLOB
            + len(dict_buf)
            + _SIZE_COUNT_METRICS
            + len(metrics_buf)
        )
        final_buf = _OutputBuffer(complete_size)
        final_buf.write_byte((_BINARY_FORMAT_VERSION >> _BITS_IN_BYTE) & _BYTE_MASK)
        final_buf.write_byte(_BINARY_FORMAT_VERSION & _BYTE_MASK)
        final_buf.write_int(len(dict_buf))
        final_buf.write_bytearray(dict_buf)
        final_buf.write_int(self._metrics_count)
        final_buf.write_bytearray(metrics_buf)
        return final_buf.to_bytearray()

    def _write_descriptor(self, descriptor):
        mask = self._calculate_descriptor_mask(descriptor)
        self._metrics_buf.write_byte(mask)

        # Only write to metrics buffer if the values below are
        # different than the values of the previous descriptor
        if mask & _MASK_PREFIX == 0:
            self._metrics_buf.write_int(self._get_dict_id(descriptor.prefix))

        if mask & _MASK_METRIC == 0:
            self._metrics_buf.write_int(self._get_dict_id(descriptor.metric))

        if mask & _MASK_DISCRIMINATOR == 0:
            self._metrics_buf.write_int(self._get_dict_id(descriptor.discriminator))

        if mask & _MASK_DISCRIMINATOR_VALUE == 0:
            self._metrics_buf.write_int(self._get_dict_id(descriptor.discriminator_value))

        if mask & _MASK_UNIT == 0:
            if descriptor.unit is None:
                self._metrics_buf.write_byte(_NULL_UNIT)
            else:
                self._metrics_buf.write_byte(descriptor.unit)

        # include excludedTargets and tags bytes for compatibility purposes
        if mask & _MASK_EXCLUDED_TARGETS == 0:
            self._metrics_buf.write_byte(0)

        if mask & _MASK_TAG_COUNT == 0:
            self._metrics_buf.write_byte(0)

        self._metrics_count += 1
        self._last_descriptor = descriptor

    def _calculate_descriptor_mask(self, descriptor):
        mask = 0
        if not self._last_descriptor:
            return mask

        last_descriptor = self._last_descriptor

        if descriptor.prefix == last_descriptor.prefix:
            mask |= _MASK_PREFIX

        if descriptor.metric == last_descriptor.metric:
            mask |= _MASK_METRIC

        if descriptor.discriminator == last_descriptor.discriminator:
            mask |= _MASK_DISCRIMINATOR

        if descriptor.discriminator_value == last_descriptor.discriminator_value:
            mask |= _MASK_DISCRIMINATOR_VALUE

        if descriptor.unit == last_descriptor.unit:
            mask |= _MASK_UNIT

        # include excludedTargets and tags bits for compatibility purposes
        mask |= _MASK_EXCLUDED_TARGETS
        mask |= _MASK_TAG_COUNT

        return mask

    def _get_dict_id(self, word):
        if not word:
            return _NULL_DICTIONARY_ID

        return self._metrics_dict.get_dict_id(word)

    def _write_metrics_dict(self):
        words = self._metrics_dict.get_words()
        self._dict_buf.write_int(len(words))

        last_word_text = ""
        for word in words:
            word_text = word.word
            max_common_len = min(len(last_word_text), len(word_text))
            common_len = 0
            while (
                common_len < max_common_len and word_text[common_len] == last_word_text[common_len]
            ):
                common_len += 1

            diff_len = len(word_text) - common_len

            self._dict_buf.write_int(word.dict_id)
            self._dict_buf.write_byte(common_len)
            self._dict_buf.write_byte(diff_len)
            for i in range(common_len, len(word_text)):
                self._dict_buf.write_char(word_text[i])

            last_word_text = word_text


class _OutputBuffer(object):
    __slots__ = ("_buf", "_pos")

    def __init__(self, size=None):
        self._buf = bytearray(size or _OUTPUT_BUFFER_INITIAL_SIZE)
        self._pos = 0

    def to_bytearray(self):
        if self._pos == len(self._buf):
            return self._buf

        return self._buf[: self._pos]

    def write_bytearray(self, buf):
        n = len(buf)
        self._ensure_available(n)
        self._buf[self._pos : self._pos + n] = buf
        self._pos += n

    def write_byte(self, value):
        self._ensure_available(BYTE_SIZE_IN_BYTES)
        BE_UINT8.pack_into(self._buf, self._pos, value & _BYTE_MASK)
        self._pos += BYTE_SIZE_IN_BYTES

    def write_char(self, value):
        self._ensure_available(CHAR_SIZE_IN_BYTES)
        BE_UINT16.pack_into(self._buf, self._pos, ord(value))
        self._pos += CHAR_SIZE_IN_BYTES

    def write_double(self, value):
        self._ensure_available(DOUBLE_SIZE_IN_BYTES)
        BE_DOUBLE.pack_into(self._buf, self._pos, value)
        self._pos += DOUBLE_SIZE_IN_BYTES

    def write_int(self, value):
        self._ensure_available(INT_SIZE_IN_BYTES)
        BE_INT.pack_into(self._buf, self._pos, value)
        self._pos += INT_SIZE_IN_BYTES

    def write_long(self, value):
        self._ensure_available(LONG_SIZE_IN_BYTES)
        BE_LONG.pack_into(self._buf, self._pos, value)
        self._pos += LONG_SIZE_IN_BYTES

    def compress(self):
        buf = self.to_bytearray()

        # Set level to 1 for best speed (less CPU overhead)
        return zlib.compress(buf, 1)

    def _ensure_available(self, size):
        if self._available() < size:
            # Grow memory more than needed
            new_size = int((self._pos + size) * _OUTPUT_BUFFER_GROW_FACTOR)
            if new_size % 2 != 0:
                new_size += 1

            new_buf = bytearray(new_size)
            new_buf[: self._pos] = self._buf
            self._buf = new_buf

    def _available(self):
        return len(self._buf) - self._pos


class _Word(object):
    __slots__ = ("word", "dict_id")

    def __init__(self, word, dict_id):
        self.word = word
        self.dict_id = dict_id


class _MetricsDictionary(object):
    """Stores word -> id mappings.

    Used by :class:`MetricsCompressor`'s dictionary-based algorithm.
    """

    __slots__ = ("_words",)

    def __init__(self):
        self._words = {}

    def get_dict_id(self, word_text):
        """Returns the dictionary id for the given word.

        If the word is not yet stored in the dictionary, the word gets stored
        and a newly assigned id is returned.

        Args:
            word_text (str): Textual representation of a word.

        Returns:
            int: The dictionary id.

        Raises:
            ValueError: If the length of the word cannot fit into unsigned
                byte range.
        """
        if len(word_text) > _MAX_WORD_LENGTH:
            raise ValueError(
                "Too long value in the metric descriptor found, maximum is %s: %s"
                % (_MAX_WORD_LENGTH, word_text)
            )

        word = self._words.get(word_text, None)
        if word:
            word_id = word.dict_id
        else:
            word_id = len(self._words)
            self._words[word_text] = _Word(word_text, word_id)

        return word_id

    def get_words(self):
        """Returns all stored word<->id mappings ordered by word.

        Returns:
            list[_Word]: Words.
        """
        words = self._words.values()
        return sorted(words, key=lambda w: w.word)

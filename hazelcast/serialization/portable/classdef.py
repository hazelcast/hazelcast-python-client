import typing

from hazelcast.errors import HazelcastSerializationError


class FieldType:
    PORTABLE = 0
    BYTE = 1
    BOOLEAN = 2
    CHAR = 3
    SHORT = 4
    INT = 5
    LONG = 6
    FLOAT = 7
    DOUBLE = 8
    UTF = 9  # Defined for backward compatibility.
    STRING = 9
    PORTABLE_ARRAY = 10
    BYTE_ARRAY = 11
    BOOLEAN_ARRAY = 12
    CHAR_ARRAY = 13
    SHORT_ARRAY = 14
    INT_ARRAY = 15
    LONG_ARRAY = 16
    FLOAT_ARRAY = 17
    DOUBLE_ARRAY = 18
    UTF_ARRAY = 19  # Defined for backward compatibility.
    STRING_ARRAY = 19
    DECIMAL = 20
    DECIMAL_ARRAY = 21
    TIME = 22
    TIME_ARRAY = 23
    DATE = 24
    DATE_ARRAY = 25
    TIMESTAMP = 26
    TIMESTAMP_ARRAY = 27
    TIMESTAMP_WITH_TIMEZONE = 28
    TIMESTAMP_WITH_TIMEZONE_ARRAY = 29


class FieldDefinition:
    def __init__(
        self,
        index: int,
        field_name: str,
        field_type: int,
        version: int,
        factory_id: int = 0,
        class_id: int = 0,
    ):
        self.index = index
        self.field_name = field_name
        self.field_type = field_type
        self.version = version
        self.factory_id = factory_id
        self.class_id = class_id

    def __eq__(self, other):
        return (
            isinstance(other, FieldDefinition)
            and self.index == other.index
            and self.field_name == other.field_name
            and self.version == other.version
            and self.factory_id == other.factory_id
            and self.class_id == other.class_id
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "FieldDefinition(ix=%s, name=%s, type=%s, version=%s, fid=%s, cid=%s)" % (
            self.index,
            self.field_name,
            self.field_type,
            self.version,
            self.factory_id,
            self.class_id,
        )


class ClassDefinition:
    def __init__(self, factory_id: int, class_id: int, version: int):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version
        self.field_defs: typing.Dict[str, FieldDefinition] = {}

    def add_field_def(self, field_def):
        self.field_defs[field_def.field_name] = field_def

    def get_field(
        self, field_name_or_index: typing.Union[int, str]
    ) -> typing.Optional[FieldDefinition]:
        if isinstance(field_name_or_index, int):
            index = field_name_or_index
            count = self.get_field_count()
            if 0 <= index < count:
                for field in self.field_defs.values():
                    if field.index == index:
                        return field
            raise IndexError("Index is out of bound. Index: %s and size: %s" % (index, count))
        else:
            return self.field_defs.get(field_name_or_index, None)

    def has_field(self, field_name: str) -> bool:
        return field_name in self.field_defs

    def get_field_names(self) -> typing.List[str]:
        return list(self.field_defs.keys())

    def get_field_type(self, field_name: str) -> int:
        fd = self.get_field(field_name)
        if fd:
            return fd.field_type
        raise ValueError("Unknown field: %s" % field_name)

    def get_field_class_id(self, field_name: str) -> int:
        fd = self.get_field(field_name)
        if fd:
            return fd.class_id
        raise ValueError("Unknown field: %s" % field_name)

    def get_field_count(self) -> int:
        return len(self.field_defs)

    def set_version_if_not_set(self, version):
        if self.version < 0:
            self.version = version

    def __eq__(self, other):
        return (
            isinstance(other, ClassDefinition)
            and self.factory_id == other.factory_id
            and self.class_id == other.class_id
            and self.version == other.version
            and self.field_defs == other.field_defs
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "fid:%s, cid:%s, v:%s, fields:%s" % (
            self.factory_id,
            self.class_id,
            self.version,
            self.field_defs,
        )

    def __hash__(self):
        return hash((self.factory_id, self.class_id, self.version))


class ClassDefinitionBuilder:
    """Builder class to construct :class:`ClassDefinition` of
    :class:`hazelcast.serialization.api.Portable` objects.

    One must make sure that the order of the fields added to this
    builder is consistent across all the usages of the Portable
    object such as the write order of the fields of the
    :func:`Portable.write_portable <hazelcast.serialization.api.Portable.write_portable>`
    method.
    """

    def __init__(self, factory_id: int, class_id: int, version: int = 0):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version

        self._index = 0
        self._done = False
        self._field_defs: typing.List[FieldDefinition] = []
        self._field_names: typing.Set[str] = set()

    def add_portable_field(
        self, field_name: str, class_def: ClassDefinition
    ) -> "ClassDefinitionBuilder":
        """Adds the field with the Portable type to the
        class definition.

        Args:
            field_name: Name of the field to add.
            class_def: Class definition of the nested Portable.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        if class_def.class_id is None or class_def.class_id == 0:
            raise ValueError("Portable class id cannot be zero!")
        self._add_field_by_type(
            field_name,
            FieldType.PORTABLE,
            class_def.version,
            class_def.factory_id,
            class_def.class_id,
        )
        return self

    def add_byte_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the byte type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.BYTE, self.version)
        return self

    def add_boolean_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the boolean type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.BOOLEAN, self.version)
        return self

    def add_char_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the char type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.CHAR, self.version)
        return self

    def add_short_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the short type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.SHORT, self.version)
        return self

    def add_int_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the int type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.INT, self.version)
        return self

    def add_long_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the long type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.LONG, self.version)
        return self

    def add_float_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the float type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.FLOAT, self.version)
        return self

    def add_double_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the double type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DOUBLE, self.version)
        return self

    def add_string_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the string type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.STRING, self.version)
        return self

    def add_utf_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the string type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.

        .. deprecated:: 4.1
            This method is deprecated and will be removed in the
            next major version. Use :func:`add_string_field` instead.
        """
        return self.add_string_field(field_name)

    def add_decimal_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the decimal type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DECIMAL, self.version)
        return self

    def add_time_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the time type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIME, self.version)
        return self

    def add_date_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the date type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DATE, self.version)
        return self

    def add_timestamp_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the timestamp type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIMESTAMP, self.version)
        return self

    def add_timestamp_with_timezone_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the timestamp with timezone type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIMESTAMP_WITH_TIMEZONE, self.version)
        return self

    def add_portable_array_field(
        self, field_name: str, class_def: ClassDefinition
    ) -> "ClassDefinitionBuilder":
        """Adds the field with the Portable array type to the
        class definition.

        Args:
            field_name: Name of the field to add.
            class_def: Class definition of the nested Portable.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        if class_def.class_id is None or class_def.class_id == 0:
            raise ValueError("Portable class id cannot be zero!")
        self._add_field_by_type(
            field_name,
            FieldType.PORTABLE_ARRAY,
            class_def.version,
            class_def.factory_id,
            class_def.class_id,
        )
        return self

    def add_byte_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the byte array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.BYTE_ARRAY, self.version)
        return self

    def add_boolean_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the boolean array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.BOOLEAN_ARRAY, self.version)
        return self

    def add_char_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the char array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.CHAR_ARRAY, self.version)
        return self

    def add_short_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the short array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.SHORT_ARRAY, self.version)
        return self

    def add_int_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the int array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.INT_ARRAY, self.version)
        return self

    def add_long_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the long array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.LONG_ARRAY, self.version)
        return self

    def add_float_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the float array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.FLOAT_ARRAY, self.version)
        return self

    def add_double_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the double array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DOUBLE_ARRAY, self.version)
        return self

    def add_string_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the string array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.STRING_ARRAY, self.version)
        return self

    def add_utf_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the string array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.

        .. deprecated:: 4.1
            This method is deprecated and will be removed in the
            next major version. Use :func:`add_string_array_field` instead.
        """
        return self.add_string_array_field(field_name)

    def add_decimal_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the decimal array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DECIMAL_ARRAY, self.version)
        return self

    def add_time_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the time array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIME_ARRAY, self.version)
        return self

    def add_date_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the date array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.DATE_ARRAY, self.version)
        return self

    def add_timestamp_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the timestamp array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIMESTAMP_ARRAY, self.version)
        return self

    def add_timestamp_with_timezone_array_field(self, field_name: str) -> "ClassDefinitionBuilder":
        """Adds the field with the timestamp with timezone array type to the
        class definition.

        Args:
            field_name: Name of the field to add.

        Returns:
            Itself for chaining.

        Raises:
            HazelcastSerializationError: If this method is called
                after :func:`build` or a field with the same
                name is already registered.
        """
        self._add_field_by_type(field_name, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY, self.version)
        return self

    def add_field_def(self, field_def):
        """
        Warnings:
              This method is not intended for public usage.
              It might be removed from the public API on the
              next major version.
        """
        if self._index != field_def.index:
            raise ValueError("Invalid field index")
        self._check(field_def.field_name)
        self._index += 1
        self._field_defs.append(field_def)
        return self

    def build(self) -> ClassDefinition:
        """Builds and returns the class definition."""
        self._done = True
        cd = ClassDefinition(self.factory_id, self.class_id, self.version)
        for field_def in self._field_defs:
            cd.add_field_def(field_def)
        return cd

    def _add_field_by_type(self, field_name, field_type, version, factory_id=0, class_id=0):
        self._check(field_name)
        fd = FieldDefinition(self._index, field_name, field_type, version, factory_id, class_id)
        self._field_defs.append(fd)
        self._index += 1

    def _check(self, field_name):
        if field_name in self._field_names:
            raise HazelcastSerializationError("Field with the name %s already exists" % field_name)
        self._field_names.add(field_name)

        if self._done:
            raise HazelcastSerializationError(
                "ClassDefinition is already built for %s" % self.class_id
            )

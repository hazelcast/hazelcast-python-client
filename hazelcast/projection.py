import typing

from hazelcast.core import MapEntry
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.types import ProjectionType, KeyType, ValueType

_PROJECTIONS_FACTORY_ID = -30


class Projection(typing.Generic[ProjectionType]):
    """Marker base class for all projections.

    Projections allow the client to transform (strip down) each query result
    object in order to avoid redundant network traffic.
    """


class _AbstractProjection(Projection[ProjectionType], IdentifiedDataSerializable):
    def write_data(self, object_data_output):
        raise NotImplementedError("write_data")

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return _PROJECTIONS_FACTORY_ID

    def get_class_id(self):
        raise NotImplementedError("get_class_id")


def _validate_attribute_path(attribute_path: str) -> None:
    if not attribute_path:
        raise ValueError("attribute_path must not be None or empty")

    if "[any]" in attribute_path:
        raise ValueError("attribute_path must not contain [any] operators")


class _SingleAttributeProjection(_AbstractProjection[ProjectionType]):
    def __init__(self, attribute_path: str):
        _validate_attribute_path(attribute_path)
        self._attribute_path = attribute_path

    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)

    def get_class_id(self):
        return 0


class _MultiAttributeProjection(_AbstractProjection[typing.List[typing.Any]]):
    def __init__(self, attribute_paths: typing.Sequence[str]):
        if not attribute_paths:
            raise ValueError("Specify at least one attribute path")

        for attribute_path in attribute_paths:
            _validate_attribute_path(attribute_path)

        self.attribute_paths = attribute_paths

    def write_data(self, object_data_output):
        object_data_output.write_string_array(self.attribute_paths)

    def get_class_id(self):
        return 1


class _IdentityProjection(_AbstractProjection[MapEntry[KeyType, ValueType]]):
    def write_data(self, object_data_output):
        pass

    def get_class_id(self):
        return 2


def single_attribute(attribute_path: str) -> Projection[ProjectionType]:
    """Creates a projection that extracts the value of
    the given attribute path.

    Args:
        attribute_path: Path to extract the attribute from.

    Returns:
        A projection that extracts the value of the given attribute path.
    """
    return _SingleAttributeProjection(attribute_path)


def multi_attribute(*attribute_paths: str) -> Projection[typing.List[typing.Any]]:
    """Creates a projection that extracts the values of
    one or more attribute paths.

    Args:
        *attribute_paths: Paths to extract the attributes from.

    Returns:
        A projection that extracts the values of the given attribute paths.
    """
    return _MultiAttributeProjection(list(attribute_paths))


def identity() -> Projection[MapEntry[KeyType, ValueType]]:
    """Creates a projection that does no transformation.

    Returns:
        A projection that does no transformation.
    """
    return _IdentityProjection()

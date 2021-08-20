from hazelcast.serialization.api import IdentifiedDataSerializable

_PROJECTIONS_FACTORY_ID = -30


class Projection(object):
    """Marker base class for all projections.

    Projections allow the client to transform (strip down) each query result
    object in order to avoid redundant network traffic.
    """

    pass


class _AbstractProjection(Projection, IdentifiedDataSerializable):
    def write_data(self, object_data_output):
        raise NotImplementedError("write_data")

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return _PROJECTIONS_FACTORY_ID

    def get_class_id(self):
        raise NotImplementedError("get_class_id")


def _validate_attribute_path(attribute_path):
    # type: (str) -> None
    if not attribute_path:
        raise ValueError("attribute_path must not be None or empty")

    if "[any]" in attribute_path:
        raise ValueError("attribute_path must not contain [any] operators")


class _SingleAttributeProjection(_AbstractProjection):
    def __init__(self, attribute_path):
        # type: (str) -> None
        _validate_attribute_path(attribute_path)
        self._attribute_path = attribute_path

    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)

    def get_class_id(self):
        return 0


class _MultiAttributeProjection(_AbstractProjection):
    def __init__(self, attribute_paths):
        # type: (list[str]) -> None
        if not attribute_paths:
            raise ValueError("Specify at least one attribute path")

        for attribute_path in attribute_paths:
            _validate_attribute_path(attribute_path)

        self.attribute_paths = attribute_paths

    def write_data(self, object_data_output):
        object_data_output.write_string_array(self.attribute_paths)

    def get_class_id(self):
        return 1


class _IdentityProjection(_AbstractProjection):
    def write_data(self, object_data_output):
        pass

    def get_class_id(self):
        return 2


def single_attribute(attribute_path):
    # type: (str) -> Projection
    """Creates a projection that extracts the value of
    the given attribute path.

    Args:
        attribute_path (str): Path to extract the attribute from.

    Returns:
        Projection[any]: A projection that extracts the value of the given
        attribute path.
    """
    return _SingleAttributeProjection(attribute_path)


def multi_attribute(*attribute_paths):
    # type: (str) -> Projection
    """Creates a projection that extracts the values of
    one or more attribute paths.

    Args:
        *attribute_paths (str): Paths to extract the attributes from.

    Returns:
        Projection[list]: A projection that extracts the values of the given
        attribute paths.
    """
    return _MultiAttributeProjection(list(attribute_paths))


def identity():
    # type: () -> Projection
    """Creates a projection that does no transformation.

    Returns:
        Projection[hazelcast.core.MapEntry]: A projection that does no
        transformation.
    """
    return _IdentityProjection()

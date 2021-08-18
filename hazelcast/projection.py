from hazelcast.serialization.api import IdentifiedDataSerializable

_PROJECTIONS_FACTORY_ID = -30

class Projection(object):
    """Marker base class for all projections.

    Projections allow the client to transform each query result object in order
    to avoid redundant network traffic. The computation is performed in a fully
    distributed manner, so no data other than the computed value is transferred
    to the client, making the computation fast.
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


class _SingleAttributeProjection(_AbstractProjection):
    def __init__(self, attribute_path):
        self._attribute_path = attribute_path

    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)

    def get_class_id(self):
        return 0


class _MultiAttributeProjection(_AbstractProjection):
    def __init__(self, *attribute_paths):
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


def single_attribute(attribute_path=None):
    """Creates a projection that extracts the value of
    an attribute path.

    Args:
        attribute_path (str): Extracts values from this path, if given.

    Returns:
        Projection[any]: A projection that extracts the value of the given
            attribute path.
    """
    return _SingleAttributeProjection(attribute_path)


def multi_attribute(*attribute_paths):
    """Creates a projection that extracts the values of
    one or more attribute paths.

    Args:
        attribute_paths (str): Extracts values from these paths, if given.

    Returns:
        Projection[any]: A projection that extracts the values of the given
            attribute paths.
    """
    return _MultiAttributeProjection(*attribute_paths)


def identity():
    """Creates a projection that does no transformation
    on the map.

    Returns:
        Projection[any]: A projection that does no transformation.
    """
    return _IdentityProjection()

from hazelcast.six.moves import range


class ErrorHolder(object):
    __slots__ = ("error_code", "class_name", "message", "stack_trace_elements")

    def __init__(self, error_code, class_name, message, stack_trace_elements):
        self.error_code = error_code
        self.class_name = class_name
        self.message = message
        self.stack_trace_elements = stack_trace_elements

    def __eq__(self, other):
        return (
            isinstance(other, ErrorHolder)
            and self.error_code == other.error_code
            and self.class_name == other.class_name
            and self.message == other.message
            and self.stack_trace_elements == other.stack_trace_elements
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class StackTraceElement(object):
    __slots__ = ("class_name", "method_name", "file_name", "line_number")

    def __init__(self, class_name, method_name, file_name, line_number):
        self.class_name = class_name
        self.method_name = method_name
        self.file_name = file_name
        self.line_number = line_number

    def __eq__(self, other):
        return (
            isinstance(other, StackTraceElement)
            and self.class_name == other.class_name
            and self.method_name == other.method_name
            and self.file_name == other.file_name
            and self.line_number == other.line_number
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class EndpointQualifier(object):
    __slots__ = ()

    def __init__(self, _, __):
        pass


class RaftGroupId(object):
    __slots__ = ("name", "seed", "id")

    def __init__(self, name, seed, group_id):
        self.name = name
        self.seed = seed
        self.id = group_id

    def __eq__(self, other):
        return (
            isinstance(other, RaftGroupId)
            and self.name == other.name
            and self.seed == other.seed
            and self.id == other.id
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.name, self.seed, self.id))

    def __repr__(self):
        return "RaftGroupId(name=%s, seed=%s, id=%s)" % (self.name, self.seed, self.id)


class AnchorDataListHolder(object):
    __slots__ = ("anchor_page_list", "anchor_data_list")

    def __init__(self, page_list, data_list):
        self.anchor_page_list = page_list
        self.anchor_data_list = data_list

    def as_anchor_list(self, to_object):
        object_list = []
        for i in range(len(self.anchor_data_list)):
            page = self.anchor_page_list[i]
            key, value = self.anchor_data_list[i]

            key = to_object(key)
            value = to_object(value)
            object_list.append((page, (key, value)))

        return object_list


class PagingPredicateHolder(object):
    __slots__ = (
        "anchor_data_list_holder",
        "predicate_data",
        "comparator_data",
        "page_size",
        "page",
        "iteration_type_id",
        "partition_key_data",
    )

    def __init__(
        self,
        anchor_data_list_holder,
        predicate_data,
        comparator_data,
        page_size,
        page,
        iteration_type_id,
        partition_key_data,
    ):
        self.anchor_data_list_holder = anchor_data_list_holder
        self.predicate_data = predicate_data
        self.comparator_data = comparator_data
        self.page_size = page_size
        self.page = page
        self.iteration_type_id = iteration_type_id
        self.partition_key_data = partition_key_data

    @staticmethod
    def of(predicate, to_data):
        anchor_list = predicate.anchor_list
        anchor_data_list = []
        page_list = []

        for page, (key, value) in anchor_list:
            page_list.append(page)
            key = to_data(key)
            value = to_data(value)
            anchor_data_list.append((key, value))

        anchor_data_list_holder = AnchorDataListHolder(page_list, anchor_data_list)
        predicate_data = to_data(predicate)
        comparator_data = to_data(predicate.comparator)
        iteration_type = predicate.iteration_type

        return PagingPredicateHolder(
            anchor_data_list_holder,
            predicate_data,
            comparator_data,
            predicate.page_size,
            predicate.page,
            iteration_type,
            None,
        )

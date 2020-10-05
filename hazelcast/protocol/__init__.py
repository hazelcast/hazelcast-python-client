class ErrorHolder(object):
    __slots__ = ("error_code", "class_name", "message", "stack_trace_elements")

    def __init__(self, error_code, class_name, message, stack_trace_elements):
        self.error_code = error_code
        self.class_name = class_name
        self.message = message
        self.stack_trace_elements = stack_trace_elements

    def __eq__(self, other):
        return isinstance(other, ErrorHolder) and self.error_code == other.error_code \
               and self.class_name == other.class_name and self.message == other.message \
               and self.stack_trace_elements == other.stack_trace_elements

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
        return isinstance(other, StackTraceElement) and self.class_name == other.class_name \
               and self.method_name == other.method_name and self.file_name == other.file_name \
               and self.line_number == other.line_number

    def __ne__(self, other):
        return not self.__eq__(other)


class EndpointQualifier(object):
    __slots__ = ()

    def __init__(self, type, identifier):
        pass

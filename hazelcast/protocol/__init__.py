class ErrorHolder(object):
    __slots__ = ("error_code", "class_name", "message", "stack_trace_elements")

    def __init__(self, error_code, class_name, message, stack_trace_elements):
        self.error_code = error_code
        self.class_name = class_name
        self.message = message
        self.stack_trace_elements = stack_trace_elements


class StackTraceElement(object):
    __slots__ = ("class_name", "method_name", "file_name", "line_number")

    def __init__(self, class_name, method_name, file_name, line_number):
        self.class_name = class_name
        self.method_name = method_name
        self.file_name = file_name
        self.line_number = line_number


class EndpointQualifier(object):
    __slots__ = ()

    def __init__(self, type, identifier):
        pass

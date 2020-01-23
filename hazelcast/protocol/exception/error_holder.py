class ErrorHolder:
    def __init__(self,error_code, class_name, message, stack_trace_elements):
        self.error_code = error_code
        self.class_name = class_name
        self.message = message
        self.stack_trace_elements = stack_trace_elements

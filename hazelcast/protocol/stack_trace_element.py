class StackTraceElement(object):
    def __init__(self, class_name, method_name, file_name, line_number):
        self.class_name = class_name
        self.method_name = method_name
        self.file_name = file_name
        self.line_number = line_number

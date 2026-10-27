def read_string_from_input(inp):
    return inp.read_string()


def write_string_to_output(out, value):
    out.write_string(value)


def read_string_from_reader(reader, field_name):
    return reader.read_string(field_name)


def write_string_to_writer(writer, field_name, value):
    writer.write_string(field_name, value)

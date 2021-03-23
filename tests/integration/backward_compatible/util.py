def read_string_from_input(inp):
    if hasattr(inp, "read_string"):
        return inp.read_string()
    else:
        return inp.read_utf()


def write_string_to_output(out, value):
    if hasattr(out, "write_string"):
        out.write_string(value)
    else:
        out.write_utf(value)


def read_string_from_reader(reader, field_name):
    if hasattr(reader, "read_string"):
        return reader.read_string(field_name)
    else:
        return reader.read_utf(field_name)


def write_string_to_writer(writer, field_name, value):
    if hasattr(writer, "write_string"):
        writer.write_string(field_name, value)
    else:
        writer.write_utf(field_name, value)

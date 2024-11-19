"""Registry of available sources."""

source_arguments = {}


def register(source, add_arguments):
    global source_arguments
    source_arguments[source] = add_arguments

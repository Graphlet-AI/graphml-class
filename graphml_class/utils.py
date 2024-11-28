import resource


def get_memory_usage():
    """Return the resident memory usage in GB."""
    return (
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    ) / (1024 * 1024)


def print_memory_usage():
    """Print the resident memory usage in GB."""
    print(f"Memory usage: {get_memory_usage():.2f} GB")

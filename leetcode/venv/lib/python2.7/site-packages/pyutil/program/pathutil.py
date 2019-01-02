import os, sys

def get_fullpath(filepath, path):
    if path:
        return os.path.abspath(os.path.join(os.path.dirname(filepath), path))
    else:
        return os.path.abspath(os.path.dirname(filepath))

def append_path(filepath, *paths):
    for path in paths:
        fullp = get_fullpath(filepath, path)
        if fullp not in sys.path:
            sys.path.append(fullp)

def prepend_path(filepath, *paths):
    for path in reversed(paths):
        fullp = get_fullpath(filepath, path)
        if fullp not in sys.path:
            sys.path.insert(0, fullp)

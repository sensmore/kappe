class ClassDict(dict):
    """Class to allow attribute access to dict items."""

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

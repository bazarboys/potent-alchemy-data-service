def dict_to_tuple(d, keys):
    """
    Converts the dictionary to a tuple using `keys` to order the values.
    """
    return tuple([d[key] for key in keys])


def tuple_to_dict(t, keys):
    """
    Converts the tuple to a dictionary using `keys` to map the values.
    """
    return {key: t[i] for i, key in enumerate(keys)}

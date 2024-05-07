def recursive_serialize(value):
    if hasattr(value, 'serialize'):
        return value.serialize()
    if isinstance(value, dict):
        output = {}
        for key in value:
            output[key] = recursive_serialize(value[key])
        return output

    if isinstance(value, list):
        output = []
        for e in value:
            output.append(recursive_serialize(e))
        return output

    else:
        return value

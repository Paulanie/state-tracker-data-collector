from typing import Dict, List, Any, MutableMapping


def get_or(d: Dict, f: str, default: Any) -> Any:
    return d.get(f, default) or default


def delete_keys_from_dict(dictionary: Dict, keys: List[str]):
    cpy = dictionary.copy()
    for field in cpy.keys():
        if field in keys:
            del dictionary[field]
        if type(cpy[field]) == dict:
            delete_keys_from_dict(dictionary[field], keys)
    return dictionary


def delete_empty_nested_from_dict(dictionary: Dict):
    cpy = dictionary.copy()
    for field in cpy.keys():
        if type(cpy[field]) == dict:
            if len(cpy[field]) <= 0:
                del dictionary[field]
            else:
                delete_empty_nested_from_dict(dictionary[field])
    return dictionary


def _flatten_dict_gen(d, parent_key, sep):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '.') -> dict:
    return dict(_flatten_dict_gen(d, parent_key, sep))

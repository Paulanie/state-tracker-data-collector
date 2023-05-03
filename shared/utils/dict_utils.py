from typing import Dict, List, Any, MutableMapping, Union, Optional


def get_or(d: Dict, f: str, default: Any) -> Any:
    return d.get(f, default) or default


def delete_keys_from_dict(dictionary: Union[Dict | List[Dict]], keys: List[str], delete_empty: bool = True):
    def __delete_keys(d: Dict):
        cpy = d.copy()
        for field in cpy.keys():
            if field in keys:
                del d[field]
            if type(cpy[field]) == dict:
                __delete_keys(d[field])
                if len(d[field]) <= 0 and delete_empty:
                    del d[field]
        return d

    if isinstance(dictionary, List):
        return [__delete_keys(d) for d in dictionary]
    else:
        return __delete_keys(dictionary)


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


def get(d: Dict, *keys: str, default: Optional[Any] = None) -> Any:
    current = d
    for k in keys:
        if k in current and current[k] is not None:
            current = current[k]
        else:
            return default
    return current

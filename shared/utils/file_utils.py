from typing import List, Dict

import orjson as orjson
from pathlib import Path


def read_jsons(paths: List[str]) -> List[Dict]:
    return [read_json(p) for p in paths]


def read_json(path: str) -> Dict:
    with open(path, "r") as f:
        return orjson.loads(f.read())


def get_all_files_in_dir(path: str, extension: str = None) -> List[str]:
    return [str(p) for p in Path(path).rglob('*' + (f".{extension}" if extension is not None else "")) if p.is_file()]

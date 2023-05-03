from typing import Any, Optional


def to_int(x: Optional[Any] = None) -> Optional[int]:
    return int(x) if x is not None else None

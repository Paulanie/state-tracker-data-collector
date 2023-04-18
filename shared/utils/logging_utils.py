import datetime
from typing import Callable, List

from tqdm_loggable.auto import tqdm


def wrap_around_progress_bar(operation: Callable, data: List, description: str = "Progress") -> List:
    output = []
    with tqdm(total=len(data), desc=description, unit_scale=True) as bar:
        for i in range(len(data)):
            output.append(operation(data[i]))

            bar.update(1)
            bar.set_postfix({"time": datetime.datetime.utcnow()})
    return output

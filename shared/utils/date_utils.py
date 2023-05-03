import datetime
from typing import Dict, Optional, Union

import dateutil.tz

from .dict_utils import get_or

TIMEZONE = dateutil.tz.gettz("Europe/Paris")


def now_with_tz():
    return datetime.datetime.now(tz=TIMEZONE)


def convert_to_datetime(data: Optional[str], date_format: Optional[str] = None, as_date: bool = False) -> \
        Optional[Union[datetime.datetime | datetime.date]]:
    if data is None or len(data) <= 0:
        return None

    if date_format is None:
        return datetime.datetime.fromisoformat(data).astimezone(tz=None)

    result = datetime.datetime.strptime(data, date_format)
    return result if not as_date else datetime.date(result.year, result.month, result.day)

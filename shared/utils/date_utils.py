import datetime

import dateutil.tz

TIMEZONE = dateutil.tz.gettz("Europe/Paris")


def now_with_tz():
    return datetime.datetime.now(tz=TIMEZONE)

import logging
from typing import MutableMapping, List, Dict

from dependency_injector.wiring import inject, Provide
from sqlalchemy import select
from sqlalchemy.orm import Session

from ...env import Environment
from ...utils import download_file, get_all_files_in_dir, delete_keys_from_dict, read_json, \
    wrap_around_progress_bar, now_with_tz
from ...components import JobsTable, Amendments, Database, insert_or_update
from dateutil import parser
import datetime

JOB_PARTITION_KEY = "amendments"
USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def map_to_entity(entry: MutableMapping) -> Amendments:
    entry = entry.get("amendement")
    entry = delete_keys_from_dict(entry, USELESS_DATA)

    return Amendments.from_data_export(entry)


def get_date(entry: MutableMapping) -> datetime.datetime:
    data = entry["amendement"].get("cycleDeVie", {}).get("dateSort", "")
    if type(data) is dict:
        data = ""
    return parser.parse(data) if len(data) > 0 else now_with_tz()


@Database.with_session
def drop_data(data: List[Dict], last_run: datetime.datetime, session: Session) -> List[Dict]:
    recent_data = [d for d in data if get_date(d) > last_run]
    already_existing = {a.uid: "" for a in session.execute(select(Amendments.uid)).all()}
    return [d for d in recent_data if d["amendement"]["uid"] not in already_existing]


@inject
def amendments(jobs: JobsTable = Provide["gateways.jobs_table"]) -> None:
    last_run = jobs.get_last_run(JOB_PARTITION_KEY).get("run_datetime")
    logging.info(f"Last run was on {last_run}")

    logging.info(f"Downloading amendments from {Environment.amendments_url}...")
    data_dir = download_file(Environment.amendments_url, auto_extract=True)
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} amendments ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    logging.info("Dropping useless data ...")
    kept_data = drop_data(json_data, last_run)
    logging.info(f"Dropped {len(json_data) - len(kept_data)} amendments.")

    if len(kept_data) > 0:
        transformed_data = wrap_around_progress_bar(lambda x: map_to_entity(x), kept_data, "Mapping data")
        insert_or_update(transformed_data, Amendments.uid)
    else:
        logging.info("No data to upsert !")

    jobs.update_last_run(JOB_PARTITION_KEY, run_datetime=now_with_tz())
    logging.info("Done")

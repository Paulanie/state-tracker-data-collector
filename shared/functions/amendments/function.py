import logging
from typing import MutableMapping, List, Dict

from dependency_injector.wiring import inject, Provide
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...env import Environment
from ...utils import download_file, get_all_files_in_dir, delete_keys_from_dict, read_json, \
    wrap_around_progress_bar, now_with_tz, TIMEZONE
from ...components import JobsTable, Amendment, Database
from dateutil import parser
import datetime

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def map_to_entity(entry: MutableMapping) -> Amendment:
    entry = entry.get("amendement")
    entry = delete_keys_from_dict(entry, USELESS_DATA)

    return Amendment.from_data_export(entry)


def get_date(entry: MutableMapping) -> datetime.datetime:
    data = entry["amendement"].get("cycleDeVie", {}).get("dateSort", "")
    if type(data) is dict:
        data = ""
    return parser.parse(data) if len(data) > 0 else now_with_tz()


@Database.with_session
def drop_data(data: List[Dict], last_run: datetime.datetime, session: Session) -> List[Dict]:
    recent_data = [d for d in data if get_date(d) > last_run]
    already_existing = {a.uid: "" for a in session.execute(select(Amendment.uid)).all()}
    return [d for d in recent_data if d["amendement"]["uid"] not in already_existing]


@Database.with_session
def insert_or_update(entities: List[Amendment], session: Session):
    already_existing = {a.uid: "" for a in session.execute(select(Amendment.uid)).all()}
    to_merge = []
    to_add = []
    for entity in entities:
        if entity.uid in already_existing:
            to_merge.append(entity)
        else:
            to_add.append(entity)

    logging.info(f"{len(to_add)} entities to add and {len(to_merge)} to merge.")
    logging.info("Adding non existing entities ...")
    session.add_all(to_add)
    session.flush()

    logging.info("Merging already existing entities")
    with session.no_autoflush:
        wrap_around_progress_bar(lambda x: session.merge(x), to_merge, "Merging entities")


@inject
def amendments(jobs: JobsTable = Provide["gateways.amendments_jobs_table"]) -> None:
    last_run = jobs.get_last_run().get("run_datetime")
    logging.info(f"Last run was on {last_run}")

    logging.info("Gathering amendments ...")
    data_dir = download_file(Environment.amendments_url, auto_extract=True)
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} amendments ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    logging.info("Dropping useless data ...")
    kept_data = drop_data(json_data, last_run)
    logging.info(f"Dropped {len(json_data) - len(kept_data)} amendments.")

    if len(kept_data) > 0:
        transformed_data = wrap_around_progress_bar(lambda x: map_to_entity(x), kept_data, "Mapping data")
        insert_or_update(transformed_data)
    else:
        logging.info("No data to upsert !")

    jobs.update_last_run(run_datetime=now_with_tz())
    logging.info("Done")

import datetime
import logging
from typing import MutableMapping, Dict

from dependency_injector.wiring import inject, Provide
from ...utils import download_file, Environment, get_all_files_in_dir, delete_keys_from_dict, read_json, \
    delete_empty_nested_from_dict, wrap_around_progress_bar, get_or, now_with_tz, TIMEZONE
from ...components import Cosmos, JobsTable, Amendment
from dateutil import parser

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def replace_date_entry_by_isotzdatetime(d: Dict, key: str):
    data = get_or(d, key, "")
    if len(data) > 0:
        d[key] = datetime.datetime.strptime(data, "%Y-%m-%d").astimezone(tz=TIMEZONE).isoformat()


def transform_entry(entry: MutableMapping) -> Amendment:
    entry = entry.get("amendement")
    entry = delete_keys_from_dict(entry, USELESS_DATA)

    return Amendment({
        "uid": entry["uid"],
        "examenRef": entry["examenRef"],
        "triAmendement": entry["triAmendement"],
        ""
    })

    entry["id"] = entry["uid"]
    entry["sort"] = get_or(entry["cycleDeVie"], "sort", "Non examiné")
    del entry["cycleDeVie"]["sort"]

    replace_date_entry_by_isotzdatetime(entry["cycleDeVie"], "dateDepot")
    replace_date_entry_by_isotzdatetime(entry["cycleDeVie"], "datePublication")

    entry["cosignataires_libelle"] = get_or(entry["signataires"], "libelle", "").replace(" ", "").split(",")
    del entry["signataires"]

    entry = delete_empty_nested_from_dict(entry)

    return entry


def get_date(entry: MutableMapping) -> datetime.datetime:
    data = entry["amendement"].get("cycleDeVie", {}).get("dateSort", "")
    if type(data) is dict:
        data = ""
    return parser.parse(data) if len(data) > 0 else now_with_tz()


@inject
def amendments(cosmos: Cosmos = Provide["gateways.cosmos_client"],
               jobs: JobsTable = Provide["gateways.amendments_jobs_table"]) -> None:
    cosmos.select_container(Environment.cosmos_database, "amendments", "/uid")

    last_run = jobs.get_last_run().get("run_datetime", now_with_tz())

    logging.info("Gathering amendments ...")
    # data_dir = download_file(Environment.amendments_url, auto_extract=True)
    data_dir = "/tmp/Amendements.json/"
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} amendments ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    logging.info("Dropping useless data ...")
    #kept_data = [d for d in json_data if get_date(d) < last_run]
    logging.info(f"Dropped {len(json_data) - len(json_data)} amendments.")

    transformed_data = wrap_around_progress_bar(lambda x: transform_entry(x), json_data, "Transforming data")

    logging.info("Upserting entities into CosmosDB ...")
    cosmos.upsert_all(transformed_data, with_progress_bar=True)

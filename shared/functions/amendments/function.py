import logging
from typing import MutableMapping, Dict

from dependency_injector.wiring import inject, Provide
from ...utils import download_file, Environment, get_all_files_in_dir, delete_keys_from_dict, read_json, \
    delete_empty_nested_from_dict, wrap_around_progress_bar, get_field, flatten_dict
from ...components import Cosmos, AzureTable
from dateutil import parser

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def transform_entry(entry: MutableMapping) -> Dict:
    entry = entry.get("amendement")
    entry = delete_keys_from_dict(entry, USELESS_DATA)

    entry["id"] = entry["uid"]
    entry["sort"] = get_field(entry["cycleDeVie"], "sort", "Non examiné")
    del entry["cycleDeVie"]["sort"]

    date_sort = get_field(entry["cycleDeVie"], "dateSort", {})
    entry["dateSort"] = parser.parse(date_sort) if len(date_sort) > 0 else ""

    entry["cosignataires_libelle"] = get_field(entry["signataires"], "libelle", "").replace(" ", "").split(",")
    del entry["signataires"]

    entry = flatten_dict(entry)
    entry = delete_empty_nested_from_dict(entry)

    return entry


def get_last_run(tables: AzureTable) -> Dict:
    tables.select_jobs_table()
    return tables.get("amendments", filters={"RunDate"})


@inject
def amendments(cosmos: Cosmos = Provide["gateways.cosmos_client"],
               tables: AzureTable = Provide["gateways.tables_client"]) -> None:
    cosmos.select_container(Environment.cosmos_database, "amendments", "/uid")

    logging.info("Gathering amendments ...")
    # data_dir = download_file(Environment.amendments_url, auto_extract=True)
    data_dir = "/tmp/Amendements.json/"
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} amendments ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    transformed_data = wrap_around_progress_bar(lambda x: transform_entry(x), json_data, "Transforming data")
    logging.info("Upserting entities into CosmosDB ...")
    cosmos.upsert_all(transformed_data, with_progress_bar=True)

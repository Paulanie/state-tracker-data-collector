import logging
from typing import List, Dict, Tuple

from sqlalchemy import select, Column
from sqlalchemy.orm import Session

from ...components import Database, Professions
from ...utils import get_all_files_in_dir, wrap_around_progress_bar, read_json, get, delete_keys_from_dict

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def split_data(data: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    data_cleaned = delete_keys_from_dict(data, USELESS_DATA)
    professions = {get(d, "acteur", "profession", "libelleCourant"): get(d, "acteur", "profession") for d in
                   data_cleaned if get(d, "acteur", "profession") is not None}
    # TODO addresses, mandates
    addresses = []
    mandates = []

    return list(professions.values()), addresses, mandates, data_cleaned


@Database.with_session
def drop_data_json_entry(data: List[Dict], id_column: Column, *data_path, session: Session) -> List[Dict]:
    already_existing = {a[id_column]: "" for a in session.execute(select(id_column)).all()}
    return [d for d in data if get(d, *data_path) not in already_existing]


def actors_task(data_dir: str) -> None:
    logging.info("Integrating actors ...")
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} actors ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    professions, addresses, mandates, actors = split_data(json_data)
    drop_data_json_entry(professions, Professions.name, "libelleCourant")

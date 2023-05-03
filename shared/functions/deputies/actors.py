import logging
from typing import List, Dict, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...components import Professions, insert_or_update, drop_data_json_entry, Actors, Database
from ...utils import get_all_files_in_dir, wrap_around_progress_bar, read_json, get, delete_keys_from_dict

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def split_data(data: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    data_cleaned = [d["acteur"] for d in delete_keys_from_dict(data, USELESS_DATA)]
    professions = {get(d, "profession", "libelleCourant"): get(d, "profession") for d in
                   data_cleaned if get(d, "profession") is not None}
    # TODO addresses, mandates
    addresses = []
    mandates = []

    return list(professions.values()), addresses, mandates, data_cleaned


def transform_professions(data: List[Dict]) -> List[Professions]:
    logging.info("Transforming professions ...")
    unique_entries = list(
        {get(d, "libelleCourant").lower() if get(d, "libelleCourant") is not None else None: d for d in data}.values())
    for e in unique_entries:
        if "libelleCourant" in e and e["libelleCourant"] is not None:
            e["libelleCourant"] = e["libelleCourant"].lower()
    to_keep = drop_data_json_entry(unique_entries, Professions.name, "libelleCourant")
    return [Professions.from_data_export(e) for e in to_keep]


def transform_addresses(data: List[Dict]) -> List:
    return []


def transform_mandates(data: List[Dict]) -> List:
    return []


@Database.with_session
def transform_actors(data: List[Dict], session: Session) -> List[Actors]:
    data_dict = {get(d, "uid", "#text"): d for d in data}
    professions = {p[0].name: p[0] for p in session.execute(select(Professions)).all()}

    actors = {a[0].uid: a[0] for a in session.execute(select(Actors)).all()}
    new_actors = [Actors.from_data_export(d) for d in data]
    for a in new_actors:
        profession = get(data_dict[a.uid], "profession", "libelleCourant")
        a.profession = professions[profession.lower() if profession is not None else None]

    to_keep = [a for a in new_actors if a.uid not in actors or a != actors[a.uid]]
    return to_keep


def actors_task(data_dir: str) -> None:
    logging.info("Integrating actors ...")
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} actors ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    professions, addresses, mandates, actors = split_data(json_data)

    for data, transform, identifier in [(professions, transform_professions, Professions.name),
                                        (actors, transform_actors, Actors.uid)]:
        transformed = transform(data)
        insert_or_update(transformed, identifier)

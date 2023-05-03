import logging
from typing import List, Dict, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...components import Professions, insert_or_update, drop_data_json_entry, Actors, Database, ActorsAddresses
from ...utils import get_all_files_in_dir, wrap_around_progress_bar, read_json, get, delete_keys_from_dict

USELESS_DATA = [
    "@xmlns",
    "@xmlns:xsi",
    "@xsi:nil"
]


def split_data(data: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    data_cleaned = [d["acteur"] for d in delete_keys_from_dict(data, USELESS_DATA)]
    professions = list({get(d, "profession", "libelleCourant"): get(d, "profession") for d in
                        data_cleaned if get(d, "profession") is not None}.values())
    # TODO addresses, mandates
    addresses = [{**address, "actorUid": get(d, "acteur", "uid", "#text")} for d in data for address in
                 get(d, "acteur", "adresses", "adresse", default=[])]
    mandates = []

    return professions, addresses, mandates, data_cleaned


def transform_professions(data: List[Dict]) -> List[Professions]:
    logging.info("Transforming professions ...")
    unique_entries = list(
        {get(d, "libelleCourant").lower() if get(d, "libelleCourant") is not None else None: d for d in data}.values())
    for e in unique_entries:
        if "libelleCourant" in e and e["libelleCourant"] is not None:
            e["libelleCourant"] = e["libelleCourant"].lower()
    to_keep = drop_data_json_entry(unique_entries, Professions.name, "libelleCourant")
    return [Professions.from_data_export(e) for e in to_keep]


@Database.with_session
def transform_addresses(data: List[Dict], session: Session) -> List[ActorsAddresses]:
    logging.info("Transforming addresses ...")
    actors = {a.uid: a for a in session.query(Actors).all()}

    addresses = {a.uid: a for a in session.query(ActorsAddresses).all()}
    new_addresses = {aa["uid"]: ActorsAddresses.from_data_export(aa) for aa in data}
    for d in data:
        if d["actorUid"] in actors:
            new_addresses[d["uid"]].actor = actors[d["actorUid"]]
    return [aa for aa in new_addresses.values() if aa.uid not in addresses or aa != addresses[aa.uid]]


def transform_mandates(data: List[Dict]) -> List:
    return []


@Database.with_session
def transform_actors(data: List[Dict], session: Session) -> List[Actors]:
    logging.info("Transforming actors ...")
    data_dict = {get(d, "uid", "#text"): d for d in data}
    professions = {p.name: p for p in session.query(Professions).all()}

    actors = {a.uid: a for a in session.query(Actors).all()}
    new_actors = [Actors.from_data_export(d) for d in data]
    for a in new_actors:
        profession = get(data_dict[a.uid], "profession", "libelleCourant")
        a.profession = professions[profession.lower() if profession is not None else None]

    return [a for a in new_actors if a.uid not in actors or a != actors[a.uid]]


def actors_task(data_dir: str) -> None:
    logging.info("Integrating actors ...")
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} actors ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    professions, addresses, mandates, actors = split_data(json_data)

    for data, transform, identifier in [(professions, transform_professions, Professions.name),
                                        (actors, transform_actors, Actors.uid),
                                        (addresses, transform_addresses, ActorsAddresses.uid)]:
        transformed = transform(data)
        insert_or_update(transformed, identifier)

import logging
from typing import List, Dict

from sqlalchemy.orm import Session

from shared.components import Database, insert_or_update, Organs
from shared.utils import get_all_files_in_dir, wrap_around_progress_bar, read_json, get


@Database.with_session
def transform_organs(data: List[Dict], session: Session) -> List[Organs]:
    logging.info("Transforming organs ...")
    data_dict = {get(d, "organe", "uid"): d["organe"] for d in data}

    organs = {o.uid: o for o in session.query(Organs).all()}
    new_organs = {uid: Organs.from_data_export(d) for uid, d in data_dict.items()}
    for uid, d in data_dict.items():
        if get(d, "organeParent") is not None:
            puid = get(d, "organeParent")
            parent = new_organs[puid] if puid in new_organs else organs[puid] if puid in organs else None
            if parent is not None:
                new_organs[uid].parent = parent
    return [o for o in new_organs.values() if o.uid not in organs or o != organs[o.uid]]


def organs_task(data_dir: str) -> None:
    logging.info("Integrating organs ...")
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} organs ! Applying transformations and filtering ...")

    json_data = wrap_around_progress_bar(lambda x: read_json(x), json_files, "Reading JSON files")
    transformed = transform_organs(json_data)

    insert_or_update(transformed, Organs.uid)

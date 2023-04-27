import logging
from ...utils import get_all_files_in_dir


def actors_task(data_dir: str) -> None:
    logging.info("Integrating actors ...")
    json_files = get_all_files_in_dir(data_dir)
    logging.info(f"Found {len(json_files)} actors ! Applying transformations and filtering ...")

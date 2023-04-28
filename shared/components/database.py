import logging
from typing import Callable, List, Dict

from sqlalchemy import create_engine, URL, Engine, select, Column
from sqlalchemy.orm import Session

from ..utils import wrap_around_progress_bar, get


class Database:
    _engine: Engine

    @classmethod
    def init_with_credentials(cls, username: str, password: str, host: str, port: int, database: str):
        cls._engine = create_engine(
            URL.create(drivername="mssql+pymssql", username=username, password=password, host=host, port=port,
                       database=database))

    @classmethod
    def with_session(cls, func: Callable):
        def session_wrapper(*args, **kwargs):
            with Session(cls._engine) as session:
                result = func(*args, **kwargs, session=session)
                logging.info("Committing transaction ...")
                session.commit()
                return result

        return session_wrapper

    @classmethod
    def get_session(cls) -> Session:
        return Session(cls._engine)


@Database.with_session
def insert_or_update(entities: List, entity_id_column: Column, session: Session):
    already_existing = {a[0]: "" for a in session.execute(select(entity_id_column)).all()}
    to_merge = []
    to_add = []
    for entity in entities:
        if entity.__dict__.get(entity_id_column.key) in already_existing:
            to_merge.append(entity)
        else:
            to_add.append(entity)

    logging.info(f"{len(to_add)} entities to add and {len(to_merge)} to merge.")
    if len(to_add) > 0:
        logging.info("Adding non existing entities ...")
        session.add_all(to_add)
        session.flush()

    if len(to_merge) > 0:
        logging.info("Merging already existing entities")
        with session.no_autoflush:
            wrap_around_progress_bar(lambda x: session.merge(x), to_merge, "Merging entities")


@Database.with_session
def drop_data_json_entry(data: List[Dict], unique_column: Column, *data_path, session: Session) -> List[Dict]:
    already_existing = {a[0]: "" for a in session.execute(select(unique_column)).all()}
    return [d for d in data if get(d, *data_path) not in already_existing]

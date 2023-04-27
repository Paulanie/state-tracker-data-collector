import logging
from typing import Callable, List

from sqlalchemy import create_engine, URL, Engine, select, Column
from sqlalchemy.orm import Session

from ..utils import wrap_around_progress_bar


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
    already_existing = {a.uid: "" for a in session.execute(select(entity_id_column)).all()}
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
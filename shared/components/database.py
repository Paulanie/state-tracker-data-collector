from typing import Any, Callable

from sqlalchemy import create_engine, URL, Column, String, DateTime, Boolean
from sqlalchemy.orm import Session, sessionmaker, declarative_base


class Database:
    _engine: Any
    _Session: sessionmaker[Session]

    @classmethod
    def init_with_credentials(cls, username: str, password: str, host: str, port: int, database: str):
        cls._engine = create_engine(
            URL.create(drivername="mssql+pymssql", username=username, password=password, host=host, port=port,
                       database=database))
        cls._Session = sessionmaker(bind=cls._engine)

    @classmethod
    def with_session(cls, func: Callable):
        def session_wrapper(*args, **kwargs):
            with cls._Session() as session:
                with session.begin():
                    return func(*args, **kwargs, session=session)

        return session_wrapper


Base = declarative_base()


class Amendment(Base):
    __tablename__ = "amendments"

    uid = Column(String, primary_key=True)
    examenRef = Column(String)
    triAmendement = Column(String)
    texteLegislatifRef = Column(String)

    dateDepot = Column(DateTime)
    datePublication = Column(DateTime)
    dateSort = Column(DateTime)

    etat = Column(String)
    sousEtat = Column(String)
    representation = Column(String)

    article99 = Column(Boolean)

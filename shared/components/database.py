import logging
from typing import Any, Callable, Dict

from sqlalchemy import create_engine, URL, Column, String, DateTime, Boolean, Engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base

from shared.utils import convert_to_datetime, get


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

    @classmethod
    def from_data_export(cls, data: Dict) -> "Amendment":
        return Amendment(**{
            "uid": data["uid"],
            "examenRef": data["examenRef"],
            "triAmendement": data["triAmendement"] if len(data["triAmendement"]) > 0 else None,
            "texteLegislatifRef": data["texteLegislatifRef"],
            "dateDepot": convert_to_datetime(data["cycleDeVie"]["dateDepot"], "%Y-%m-%d"),
            "datePublication": convert_to_datetime(data["cycleDeVie"].get("datePublication"), "%Y-%m-%d"),
            "dateSort": convert_to_datetime(data["cycleDeVie"].get("dateSort")),
            "etat": data["cycleDeVie"]["etatDesTraitements"]["etat"]["libelle"],
            "sousEtat": data["cycleDeVie"]["etatDesTraitements"]["sousEtat"].get("libelle"),
            "representation": get(data, "representations", "representation", "contenu", "documentURI"),
            "article99": data["article99"].lower() == "true"
        })


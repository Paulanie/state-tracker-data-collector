from typing import Dict

from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base

from shared.utils import convert_to_datetime, get

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


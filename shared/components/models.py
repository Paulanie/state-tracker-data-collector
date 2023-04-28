from typing import Dict, List, Optional

from sqlalchemy import Column, String, DateTime, Boolean, Date, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, Mapped, relationship, mapped_column

from shared.utils import convert_to_datetime, get

Base = declarative_base()


class Amendments(Base):
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
    def from_data_export(cls, data: Dict) -> "Amendments":
        return Amendments(**{
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


class Actors(Base):
    __tablename__ = "actors"

    uid = Column(String, primary_key=True)
    title = Column(String)
    surname = Column(String)
    name = Column(String)
    alpha = Column(String)
    trigram = Column(String)
    birthdate = Column(Date)
    birthplace = Column(String)
    deathDate = Column(Date)
    uriHatvp = Column(String)

    professionId: Mapped[int] = mapped_column(ForeignKey("professions.id"))
    profession: Mapped[Optional["Professions"]] = relationship(back_populates="actors")

    @classmethod
    def from_data_export(cls, data: Dict) -> "Actors":
        return Actors(**{
            "uid": data["uid"]["#text"],
            "title": data["etatCivil"]["ident"]["civ"],
            "surname": data["etatCivil"]["ident"]["nom"],
            "name": data["etatCivil"]["ident"]["prenom"],
            "alpha": data["etatCivil"]["ident"]["alpha"],
            "trigram": data["etatCivil"]["ident"]["trigramme"],
            "birthdate": convert_to_datetime(data["etatCivil"]["infoNaissance"], "%Y-%m-%d"),
            "birthplace": f"{data['etatCivil']['infoNaissance']['villeNais']},"
                          f"{data['etatCivil']['infoNaissance']['depNais']},"
                          f"{data['etatCivil']['infoNaissance']['paysNais']}",
            "deathDate": convert_to_datetime(data["etatCivil"].get("dateDeces"), "%Y-%m-%d"),
            "uriHatvp": get(data, "uriHatvp")
        })


class Professions(Base):
    __tablename__ = "professions"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    family = Column(String)
    category = Column(String)

    actors: Mapped[List["Actors"]] = relationship(back_populates="profession")

    @classmethod
    def from_data_export(cls, data: Dict) -> "Professions":
        return Professions(**{
            "name": data["libelleCourant"],
            "category": data["socProcINSEE"]["catSocPro"],
            "family": data["socProcINSEE"]["famSocPro"]
        })

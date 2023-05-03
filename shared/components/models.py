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
        birthplace = [get(data, "etatCivil", "infoNaissance", f) for f in ["villeNais", "depNais", "paysNais"]]
        birthplace = ','.join([b for b in birthplace if b is not None])
        return Actors(**{
            "uid": data["uid"]["#text"],
            "title": data["etatCivil"]["ident"]["civ"],
            "surname": data["etatCivil"]["ident"]["nom"],
            "name": data["etatCivil"]["ident"]["prenom"],
            "alpha": data["etatCivil"]["ident"]["alpha"],
            "trigram": get(data, "etatCivil", "ident", "trigramme"),
            "birthdate": convert_to_datetime(get(data, "etatCivil", "infoNaissance", "dateNais"), "%Y-%m-%d",
                                             as_date=True),
            "birthplace": birthplace if len(birthplace) > 0 else None,
            "deathDate": convert_to_datetime(data["etatCivil"].get("dateDeces"), "%Y-%m-%d", as_date=True),
            "uriHatvp": get(data, "uri_hatvp")
        })

    def __eq__(self, other: "Actors"):
        to_compare = [k for k in (self.__dict__.keys() & other.__dict__.keys()) if not k.startswith("_")]
        return all([self.__dict__[k] == other.__dict__[k] for k in to_compare])


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
            "name": get(data, "libelleCourant").lower() if get(data, "libelleCourant") is not None else None,
            "category": data["socProcINSEE"]["catSocPro"],
            "family": data["socProcINSEE"]["famSocPro"]
        })

    def __eq__(self, other: "Professions"):
        return self.id == other.id

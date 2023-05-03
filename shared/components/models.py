from typing import Dict, List, Optional

from sqlalchemy import Column, String, DateTime, Boolean, Date, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, Mapped, relationship, mapped_column

from shared.utils import convert_to_datetime, get, to_int

Base = declarative_base()


class Amendments(Base):
    __tablename__ = "amendments"

    uid = Column(String, primary_key=True)
    examinationRef = Column(String)
    triAmendment = Column(String)
    legislativeTextRef = Column(String)

    deliveryDate = Column(DateTime)
    publicationDate = Column(DateTime)
    sortDate = Column(DateTime)

    state = Column(String)
    subState = Column(String)
    representation = Column(String)

    article99 = Column(Boolean)

    @classmethod
    def from_data_export(cls, data: Dict) -> "Amendments":
        return Amendments(**{
            "uid": data["uid"],
            "examinationRef": data["examenRef"],
            "triAmendment": get(data, "triAmendement") if get(data, "triAmendement") is not None and len(
                get(data, "triAmendement")) > 0 else None,
            "legislativeTextRef": data["texteLegislatifRef"],
            "deliveryDate": convert_to_datetime(get(data, "cycleDeVie", "dateDepot"), "%Y-%m-%d"),
            "publicationDate": convert_to_datetime(get(data, "cycleDeVie", "datePublication"), "%Y-%m-%d"),
            "sortDate": convert_to_datetime(get(data, "cycleDeVie", "dateSort")),
            "state": get(data, "cycleDeVie", "etatDesTraitements", "etat", "libelle"),
            "subState": get(data, "cycleDeVie", "etatDesTraitements", "sousEtat", "libelle"),
            "representation": get(data, "representations", "representation", "contenu", "documentURI"),
            "article99": data["article99"].lower() == "true"
        })


class Organs(Base):
    __tablename__ = "organs"

    uid = Column(String, primary_key=True)
    type = Column(String)
    label = Column(String)
    editionLabel = Column(String)
    shortLabel = Column(String)
    abbreviationLabel = Column(String)
    viMoDeStartDate = Column(Date, nullable=True)
    viMoDeEndDate = Column(Date, nullable=True)
    viMoDeApprovalDate = Column(Date, nullable=True)
    chamber = Column(String, nullable=True)
    regime = Column(String)
    legislature = Column(Integer)
    number = Column(Integer)
    regionType = Column(String, nullable=True)
    regionLabel = Column(String, nullable=True)
    departmentCode = Column(String, nullable=True)
    departmentLabel = Column(String, nullable=True)

    parentOrganUid: Mapped[str] = mapped_column(ForeignKey("organs.uid"))
    parent: Mapped[Optional["Organs"]] = relationship(remote_side=uid)
    children: Mapped[List["Organs"]] = relationship(back_populates="parent")

    @classmethod
    def from_data_export(cls, data: Dict) -> "Organs":
        return Organs(**{
            "uid": data["uid"],
            "type": data["codeType"],
            "label": data["libelle"],
            "editionLabel": get(data, "libelleEdition"),
            "shortLabel": get(data, "libelleAbrege"),
            "abbreviationLabel": get(data, "libelleAbrev"),
            "viMoDeStartDate": convert_to_datetime(get(data, "viMoDe", "dateDebut"), "%Y-%m-%d", as_date=True),
            "viMoDeEndDate": convert_to_datetime(get(data, "viMoDe", "dateFin"), "%Y-%m-%d", as_date=True),
            "viMoDeApprovalDate": convert_to_datetime(get(data, "viMoDe", "dateAgrement"), "%Y-%m-%d", as_date=True),
            "chamber": get(data, "chambre"),
            "regime": get(data, "regime"),
            "legislature": to_int(get(data, "legislature")),
            "number": to_int(get(data, "numero")),
            "regionType": get(data, "lieu", "region", "type"),
            "regionLabel": get(data, "lieu", "region", "libelle"),
            "departmentCode": get(data, "lieu", "departement", "code"),
            "departmentLabel": get(data, "lieu", "departement", "libelle")
        })

    def __eq__(self, other: "Organs"):
        to_compare = [k for k in (self.__dict__.keys() & other.__dict__.keys()) if not k.startswith("_")]
        return all([self.__dict__[k] == other.__dict__[k] for k in to_compare])


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
    addresses: Mapped[List["ActorsAddresses"]] = relationship(back_populates="actor")

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


class ActorsAddresses(Base):
    __tablename__ = "actorsaddresses"

    uid = Column(String, primary_key=True)
    type = Column(Integer)
    typeName = Column(String)
    weight = Column(Integer, nullable=True)
    affiliateAddress = Column(String, nullable=True)
    streetNumber = Column(String, nullable=True)
    streetName = Column(String, nullable=True)
    zipCode = Column(String, nullable=True)
    city = Column(String, nullable=True)
    address = Column(String, nullable=True)
    phone = Column(String, nullable=True)

    actorUid: Mapped[str] = mapped_column(ForeignKey("actors.uid"))
    actor: Mapped["Actors"] = relationship(back_populates="addresses")

    @classmethod
    def from_data_export(cls, data: Dict) -> "ActorsAddresses":
        return ActorsAddresses(**{
            "uid": data["uid"],
            "type": to_int(get(data, "type")),
            "typeName": data["typeLibelle"],
            "weight": to_int(get(data, "poids")),
            "affiliateAddress": get(data, "adresseDeRattachement"),
            "streetNumber": get(data, "numeroRue"),
            "streetName": get(data, "nomRue"),
            "zipCode": get(data, "codePostal"),
            "city": get(data, "ville"),
            "address": get(data, "valElec") if data["type"] in ["22", "15"] else None,
            "phone": get(data, "valElec") if data["type"] == "11" else None
        })

    def __eq__(self, other: "ActorsAddresses"):
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

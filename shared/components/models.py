from typing import Dict, List, Optional

from sqlalchemy import Column, String, DateTime, Boolean, Date, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, Mapped, relationship, mapped_column

from shared.utils import convert_to_datetime, get, to_int

Base = declarative_base()


class Amendments(Base):
    __tablename__ = "amendments"

    uid = Column(String, primary_key=True)
    examination_ref = Column(String)
    tri_amendment = Column(String)
    legislative_text_ref = Column(String)

    delivery_date = Column(DateTime)
    publication_date = Column(DateTime)
    sort_date = Column(DateTime)

    state = Column(String)
    sub_state = Column(String)
    representation = Column(String)

    article99 = Column(Boolean)

    @classmethod
    def from_data_export(cls, data: Dict) -> "Amendments":
        return Amendments(**{
            "uid": data["uid"],
            "examination_ref": data["examenRef"],
            "tri_amendment": get(data, "triAmendement") if get(data, "triAmendement") is not None and len(
                get(data, "triAmendement")) > 0 else None,
            "legislative_text_ref": data["texteLegislatifRef"],
            "delivery_date": convert_to_datetime(get(data, "cycleDeVie", "dateDepot"), "%Y-%m-%d"),
            "publication_date": convert_to_datetime(get(data, "cycleDeVie", "datePublication"), "%Y-%m-%d"),
            "sort_date": convert_to_datetime(get(data, "cycleDeVie", "dateSort")),
            "state": get(data, "cycleDeVie", "etatDesTraitements", "etat", "libelle"),
            "sub_state": get(data, "cycleDeVie", "etatDesTraitements", "sousEtat", "libelle"),
            "representation": get(data, "representations", "representation", "contenu", "documentURI"),
            "article99": data["article99"].lower() == "true"
        })


class Organs(Base):
    __tablename__ = "organs"

    uid = Column(String, primary_key=True)
    type = Column(String)
    label = Column(String)
    edition_label = Column(String)
    short_label = Column(String)
    abbreviation_label = Column(String)
    vi_mo_de_start_date = Column(Date, nullable=True)
    vi_mo_de_end_date = Column(Date, nullable=True)
    vi_mo_de_approval_date = Column(Date, nullable=True)
    chamber = Column(String, nullable=True)
    regime = Column(String)
    legislature = Column(Integer)
    number = Column(Integer)
    region_type = Column(String, nullable=True)
    region_label = Column(String, nullable=True)
    department_code = Column(String, nullable=True)
    department_label = Column(String, nullable=True)

    parent_organ_uid: Mapped[str] = mapped_column(ForeignKey("organs.uid"))
    parent: Mapped[Optional["Organs"]] = relationship(remote_side=uid)
    children: Mapped[List["Organs"]] = relationship(back_populates="parent")

    @classmethod
    def from_data_export(cls, data: Dict) -> "Organs":
        return Organs(**{
            "uid": data["uid"],
            "type": data["codeType"],
            "label": data["libelle"],
            "edition_label": get(data, "libelleEdition"),
            "short_label": get(data, "libelleAbrege"),
            "abbreviation_label": get(data, "libelleAbrev"),
            "vi_mo_de_start_date": convert_to_datetime(get(data, "viMoDe", "dateDebut"), "%Y-%m-%d", as_date=True),
            "vi_mo_de_end_date": convert_to_datetime(get(data, "viMoDe", "dateFin"), "%Y-%m-%d", as_date=True),
            "vi_mo_de_approval_date": convert_to_datetime(get(data, "viMoDe", "dateAgrement"), "%Y-%m-%d", as_date=True),
            "chamber": get(data, "chambre"),
            "regime": get(data, "regime"),
            "legislature": to_int(get(data, "legislature")),
            "number": to_int(get(data, "numero")),
            "region_type": get(data, "lieu", "region", "type"),
            "region_label": get(data, "lieu", "region", "libelle"),
            "department_code": get(data, "lieu", "departement", "code"),
            "department_label": get(data, "lieu", "departement", "libelle")
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
    death_date = Column(Date)
    uri_hatvp = Column(String)

    profession_id: Mapped[int] = mapped_column(ForeignKey("professions.id"))
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
            "death_date": convert_to_datetime(data["etatCivil"].get("dateDeces"), "%Y-%m-%d", as_date=True),
            "uri_hatvp": get(data, "uri_hatvp")
        })

    def __eq__(self, other: "Actors"):
        to_compare = [k for k in (self.__dict__.keys() & other.__dict__.keys()) if not k.startswith("_")]
        return all([self.__dict__[k] == other.__dict__[k] for k in to_compare])


class ActorsAddresses(Base):
    __tablename__ = "actors_addresses"

    uid = Column(String, primary_key=True)
    type = Column(Integer)
    type_name = Column(String)
    weight = Column(Integer, nullable=True)
    affiliate_address = Column(String, nullable=True)
    street_number = Column(String, nullable=True)
    street_name = Column(String, nullable=True)
    zip_code = Column(String, nullable=True)
    city = Column(String, nullable=True)
    address = Column(String, nullable=True)
    phone = Column(String, nullable=True)

    actor_uid: Mapped[str] = mapped_column(ForeignKey("actors.uid"))
    actor: Mapped["Actors"] = relationship(back_populates="addresses")

    @classmethod
    def from_data_export(cls, data: Dict) -> "ActorsAddresses":
        return ActorsAddresses(**{
            "uid": data["uid"],
            "type": to_int(get(data, "type")),
            "type_name": data["typeLibelle"],
            "weight": to_int(get(data, "poids")),
            "affiliate_address": get(data, "adresseDeRattachement"),
            "street_number": get(data, "numeroRue"),
            "street_name": get(data, "nomRue"),
            "zip_code": get(data, "codePostal"),
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

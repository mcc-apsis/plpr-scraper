# coding: utf-8
from __future__ import print_function
import os, sys
import django
import re
import logging
import requests
import dataset
import datetime
from lxml import html
from urllib.parse import urljoin
# Extract agenda numbers not part of normdatei
from normality import normalize
from normdatei.text import clean_text, clean_name, fingerprint#, extract_agenda_numbers
from normdatei.parties import search_party_names, PARTIES_REGEX
from bs4 import BeautifulSoup

import pprint

sys.path.append('/home/galm/software/django/tmv/BasicBrowser/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
django.setup()

from parliament.models import *
from cities.models import *

import xml.etree.ElementTree as ET

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

MDB_LINK = 'http://www.bundestag.de/blob/472878/c1a07a64c9ea8c687df6634f2d9d805b/mdb-stammdaten-data.zip'
MDB_FNAME = 'data/mdbs/MDB_STAMMDATEN.XML'

LANDS = {
    'BWG': 'Baden-Württemberg',
}

PARTY_NAMES = [
    {'name':'cducsu','alt_names':['CDU','CSU','Fraktion der Christlich Demokratischen Union/Christlich - Sozialen Union']},
    {'name': 'linke', 'alt_names': ['DIE LINKE','Gruppe der Partei des Demokratischen Sozialismus/Linke Liste','DIE LINKE.','Fraktion DIE LINKE.']},
    {'name': 'fdp', 'alt_names': ['fdp','FDP']},
    {'name': 'spd', 'alt_names': ['spd','SPD']},
    {'name':'gruene', 'alt_names': ['Gruene']},
    {'name':'afd', 'alt_names': ['AfD']}
    #{'name':'', 'alt_names': []},
]

parl, created = Parl.objects.get_or_create(
    country=Country.objects.get(name="Germany"),
    level='N'
)

def update_parties():
    for p in PARTY_NAMES:
        party = Party.objects.get(name=p['name'])
        party.alt_names = p['alt_names']
        party.save()

def fetch_mdb_data():
    if not os.path.exists(MDB_FNAME):
        print("fetching data from bundesta website")
        url = urlopen(MDB_LINK)
        zipfile = ZipFile(BytesIO(url.read()))
        zipfile.extractall("data/mdbs")
        print(zipfile.namelist())
    return

def german_date(str):
    if str is None:
        return None
    return datetime.datetime.strptime(str,"%d.%m.%Y").date()

def parse_mdb_data():
    tree = ET.parse(MDB_FNAME)
    root = tree.getroot()

    print(root)
    for mdb in root:
        if mdb.tag =="VERSION":
            continue
        names = mdb.find('NAMEN/NAME')
        biodata = mdb.find('BIOGRAFISCHE_ANGABEN')
        person,created = Person.objects.get_or_create(
            surname=names.find('NACHNAME').text,
            first_name=names.find('VORNAME').text,
            dob = german_date(biodata.find('GEBURTSDATUM').text)
        )
        person.title = names.find('ANREDE_TITEL').text
        person.clean_name = "{} {}".format(
            person.first_name,
            person.surname
        ).strip()
        if person.title is not None:
            person.clean_name = person.title + " " + person.clean_name
        print(person)
        person.academic_title = names.find('AKAD_TITEL').text
        person.ortszusatz = names.find('ORTSZUSATZ').text
        if person.ortszusatz is not None:
            person.clean_name += "({})".format(ortszusatz)
        person.adel = names.find('ADEL').text
        if person.adel is not None:
            print(person)
            print(person.adel)

        person.year_of_birth=person.dob.year
        person.place_of_birth = biodata.find('GEBURTSORT').text
        if biodata.find('GEBURTSLAND').text is None:
            person.country_of_birth = cities.models.Country.objects.get(name="Germany")
        else:
            try:
                person.country_of_birth = cities.models.Country.objects.get(
                    name=biodata.find('GEBURTSLAND').text
                )
            except:
                print(biodata.find('GEBURTSLAND').text)
        person.date_of_death = german_date(biodata.find('STERBEDATUM').text)
        if biodata.find('GESCHLECHT').text == "männlich":
            person.gender = Person.MALE
        elif biodata.find('GESCHLECHT').text == "weiblich":
            person.gender = Person.FEMALE
        else:
            print(biodata.find('GESCHLECHT').text)
        person.family_status = biodata.find('FAMILIENSTAND').text
        person.religion = biodata.find('RELIGION').text
        person.occupation = biodata.find('BERUF').text
        person.short_bio = biodata.find('VITA_KURZ').text
        person.party = Party.objects.get(alt_names__contains=[biodata.find('PARTEI_KURZ').text])

        person.save()

        for wp in mdb.findall('WAHLPERIODEN/WAHLPERIODE'):
            ps, created = ParlSession.objects.get_or_create(
                parliament=parl,
                n=wp.find('WP').text
            )
            seat, created = Seat.objects.get_or_create(
                parlsession=ps,
                occupant=person
            )
            seat.start_date = german_date(wp.find('MDBWP_VON').text)
            seat.end_date = german_date(wp.find('MDBWP_BIS').text)
            party = Party.objects.get(
                alt_names__contains=[wp.find('INSTITUTIONEN/INSTITUTION/INS_LANG').text]
            )
            if wp.find('MANDATSART').text == "Direktwahl":
                seat.seat_type = Seat.DIRECT
                wk, created = Constituency.objects.get_or_create(
                    parliament=parl,
                    number=wp.find('WKR_NUMMER').text,
                    name=wp.find('WKR_NAME').text,
                    region=Region.objects.get(name=LANDS[wp.find('WKR_LAND').text])
                )
                seat.constituency=wk
            elif w.find('MANDATSART').text == "Landesliste":
                seat.seat_type = Seat.LIST
                pl, created = PartyList.objects.get_or_create(
                    parlsession=ps,
                    region=Region.objects.get(name=LANDS[wp.find('LISTE').text])
                )
            else:
                print(w.find('MANDATSART').text)
                break



        break


if __name__ == '__main__':
    Constituency.objects.all().delete()
    PartyList.objects.all().delete()
    Seat.objects.all().delete()
    #Person.objects.all().delete()
    fetch_mdb_data()
    update_parties()
    parse_mdb_data()

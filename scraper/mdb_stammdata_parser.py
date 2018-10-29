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
from bs4 import BeautifulSoup
import platform
import pandas as pd

import pprint

if platform.node() == "mcc-apsis":
    sys.path.append('/home/muef/tmv/BasicBrowser/')
    data_dir = '/home/muef/plpr-scraper/plenarprotokolle'
    country_table_file = "/home/muef/plpr-scraper/data/country_translations.csv"
else:
    # local paths
    sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')
    data_dir = '/media/Data/MCC/Parliament Germany/Plenarprotokolle'
    country_table_file = "/media/Data/MCC/Parliament Germany/country_translations.csv"


# imports and settings for django and database
# --------------------------------------------

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
# alternatively
# settings.configure(DEBUG=True)
django.setup()

from parliament.models import *
from cities.models import *

import xml.etree.ElementTree as ET

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

map_countries = True

if map_countries:
    country_table = pd.read_csv(country_table_file, sep=";")
    country_special = {"Russland": "Russia",
                       "CSSR": "Czechia",
                       "USA": "United States",
                       "Jugoslawien": "Serbia",
                       "Oberösterreich": "Austria",
                       "Perú": "Peru",
                       "Tschechoslowakei": "Czechia",
                       "CSFR": "Czechia",
                       "(damalige) CSR": "Czechia",
                       "Ceylon": "India",
                       "ehem. Deutsch-Ost-Afrika": "Tanzania",
                       "Tonga-Insel, Südsee": "Tonga",
                       "Südwestafrika": "Namibia",
                       "Estland": "Estonia",
                       "Tansania (ehem. Deutsch - Ost - Afrika)": "Tanzania",
                       "Tansania (ehem. Deutsch-Ost-Afrika)": "Tanzania",
                       "Niederösterreich": "Austria",
                       }


MDB_LINK = 'http://www.bundestag.de/blob/472878/c1a07a64c9ea8c687df6634f2d9d805b/mdb-stammdaten-data.zip'
MDB_FNAME = '../data/mdbs/MDB_STAMMDATEN.XML'

LANDS = {
    'BWG': "Baden-Württemberg",
    'WBB': "Baden-Württemberg", # eigentlich Württemberg-Baden (1949 - 1952)
    'WBH': "Baden-Württemberg", # eigentlich Württemberg-Hohenzollern (1949 - 1952)
    'BAD': "Baden-Württemberg", # eigentlich Baden (1949-1952)
    'BW':  "Baden-Württemberg",
    'BAY': "Bavaria",
    'BY':  "Bavaria",
    'BLW': "Berlin", # Berlin West
    'BLN': "Berlin",
    'BE':  "Berlin",
    'BRA': "Brandenburg",
    'BB':  "Brandenburg",
    'BRE': "Bremen",
    'HB':  "Bremen",
    'HBG': "Hamburg",
    'HH':  "Hamburg",
    'HES': "Hesse",
    'HE':  "Hesse",
    'MBV': "Mecklenburg-Vorpommern",
    'MV':  "Mecklenburg-Vorpommern",
    'NDS': "Lower Saxony",
    'NI':  "Lower Saxony",
    'NRW': "North Rhine-Westphalia",
    'NW':  "North Rhine-Westphalia",
    'RPF': "Rheinland-Pfalz",
    'RP':  "Rheinland-Pfalz",
    'SLD': "Saarland",
    'SL':  "Saarland",
    'SAC': "Saxony",
    'SN':  "Saxony",
    'SAA': "Saxony-Anhalt",
    'ST':  "Saxony-Anhalt",
    'SWH': "Schleswig-Holstein",
    'SH':  "Schleswig-Holstein",
    'THÜ': "Thuringia",
    'TH':  "Thuringia"
}


PARTY_NAMES = [
    {'name':'cducsu','alt_names':['CDU','CSU','CDU/CSU', 'Fraktion der Christlich Demokratischen Union/Christlich - Sozialen Union',
                                  'Fraktion der CDU/CSU (Gast)', 'CSUS', 'DSU']},
                                # all mdbs of dsu were guests with CDU
    {'name': 'linke', 'alt_names': ['DIE LINKE','Gruppe der Partei des Demokratischen Sozialismus/Linke Liste','DIE LINKE.',
                                    'Fraktion DIE LINKE.', 'PDS/LL']},
    {'name': 'fdp', 'alt_names': ['fdp','FDP', 'Fraktion der Freien Demokratischen Partei',
                                  'Fraktion der FDP (Gast)', 'DPS']},
                                # DPS: Demokratische Partei Saar: Landesverband Saarland der FDP
    {'name': 'spd', 'alt_names': ['spd','SPD', 'Fraktion der Sozialdemokratischen Partei Deutschlands',
                                  'Fraktion der SPD (Gast)']},
    {'name':'gruene', 'alt_names': ['Gruene', 'GRÜNE', 'Fraktion Die Grünen', 'BÜNDNIS 90/DIE GRÜNEN',
                                    'Fraktion Bündnis 90/Die Grünen', 'Gruppe Bündnis 90/Die Grünen',
                                    'Fraktion Die Grünen/Bündnis 90', 'DIE GRÜNEN/BÜNDNIS 90']},
    {'name':'afd', 'alt_names': ['AfD', 'Alternative für Deutschland', 'Blaue']},
    {'name': 'kpd', 'alt_names': ['kpd', 'KPD', 'Fraktion der Kommunistischen Partei Deutschlands']},
    {'name': 'bp', 'alt_names': ['bp', 'BP', 'Fraktion Bayernpartei', 'Fraktion Deutsche Partei Bayern',
                                 'Fraktion Deutsche Partei/Deutsche Partei Bayern']},
    {'name': 'dp', 'alt_names': ['dp', 'DP', 'Fraktion Deutsche Partei', 'Fraktion Deutsche Partei/Freie Volkspartei',
                                 'Fraktion Freie Volkspartei', 'DPB', 'Fraktion DP/DPB (Gast)',
                                 'Fraktion Demokratische Arbeitsgemeinschaft']},
                                # Freie Volkpartei merged with DP in 1957 after splitting from FDP in 1956
    {'name': 'pds', 'alt_names': ['pds', 'PDS', 'Gruppe der Partei des Demokratischen Sozialismus',
                                  'Fraktion der Partei des Demokratischen Sozialismus']},
    {'name': 'gb/bhe', 'alt_names': ['gb/bhe', 'GB/BHE', 'GB/ BHE',
                                     'Fraktion Gesamtdeutscher Block / Block der Heimatvertriebenen und Entrechteten',
                                     'Fraktion Deutscher Gemeinschaftsblock der Heimatvertriebenen und Entrechteten']},
    {'name': 'dzp', 'alt_names': ['dzp', 'DZP', 'Fraktion Deutsche Zentrums-Partei', 'FU',
                                  'Fraktion Föderalistische Union']},
    # Fraktion Förderalistische Union (FU) was not a party but a faction of DZP and BP
    {'name': 'drp', 'alt_names': ['drp', 'DRP', 'Fraktion Deutsche Reichspartei/Nationale Rechte',
                                  'Fraktion Deutsche Reichspartei', 'Fraktion DRP (Gast)']},
    {'name': 'wav', 'alt_names': ['WAV', 'Fraktion Wirtschaftliche Aufbauvereinigung', 'Fraktion WAV (Gast)']},
    {'name': 'other', 'alt_names': ['SRP', 'CVP', 'Gruppe Kraft/Oberländer', 'Gruppe Deutsche Partei']},
    # {'name':'', 'alt_names': []},
    {'name': 'parteilos', 'alt_names': ['parteilos', 'Parteilos']},
    {'name': 'fraktionslos', 'alt_names': ['fraktionslos', 'Fraktionslos']}

]

parl, created = Parl.objects.get_or_create(
    country=Country.objects.get(name="Germany"),
    level='N'
)


def update_parties():
    for p in PARTY_NAMES:
        try:
            party = Party.objects.get(name=p['name'])
        except Party.DoesNotExist:
            party = Party(name=p['name'])

        party.alt_names = p['alt_names']
        party.save()


def fetch_mdb_data():
    if not os.path.exists(MDB_FNAME):
        print("fetching data from bundestag website")
        url = urlopen(MDB_LINK)
        zipfile = ZipFile(BytesIO(url.read()))
        zipfile.extractall("data/mdbs")
        print(zipfile.namelist())
    return


def german_date(str):
    if str is None:
        return None
    return datetime.datetime.strptime(str,"%d.%m.%Y").date()


def parse_mdb_data(verbosity=0):

    warn = 0

    tree = ET.parse(MDB_FNAME)
    print("read data from {}".format(MDB_FNAME))
    root = tree.getroot()

    print(root)

    # going through entries for mdbs
    for mdb in root:
        if mdb.tag == "VERSION":
            continue
        names = mdb.find('NAMEN')
        biodata = mdb.find('BIOGRAFISCHE_ANGABEN')

        # use the last entry of names for identification
        person = Person(
            surname=names[-1].find('NACHNAME').text,
            first_name=names[-1].find('VORNAME').text,
            dob=german_date(biodata.find('GEBURTSDATUM').text)
            )
        person.title = names[-1].find('ANREDE_TITEL').text
        person.academic_title = names[-1].find('AKAD_TITEL').text
        ortszusatz = names[-1].find('ORTSZUSATZ').text
        if ortszusatz is not None:
            person.ortszusatz = ortszusatz.strip('() ')

        person.adel = names[-1].find('ADEL').text

        surnames_list = []
        firstnames_list = []

        for name in names:
            surname = name.find('NACHNAME').text

            if isinstance(surname, str):
                surnames_list.append(surname)

                if len(surname.split(' ')) > 1:
                    surnames_list.append(surname.split(" ")[-1])

            firstname = name.find('VORNAME').text

            if isinstance(firstname, str):
                firstnames_list.append(firstname)

                if len(firstname.split(' ')) > 1:
                    for fname in firstname.split(" "):
                        if len(fname) > 2:
                            firstnames_list.append(fname)

        person.alt_surnames = list(set(surnames_list))
        person.alt_first_names = list(set(firstnames_list))

        if verbosity > 0:
            # print name
            if person.adel is None:
                print("Person: {}".format(person))
            else:
                print("Person: {} {}".format(person.adel, person))

        person.year_of_birth=person.dob.year
        person.place_of_birth = biodata.find('GEBURTSORT').text
        if biodata.find('GEBURTSLAND').text is None:
            person.country_of_birth = cities.models.Country.objects.get(name="Germany")
        else:
            country_of_birth = biodata.find('GEBURTSLAND').text
            if verbosity > 0:
                print("country_of_birth: {}".format(country_of_birth))
            try:
                person.country_of_birth = cities.models.Country.objects.get(
                    name=country_of_birth
                )
            except Country.DoesNotExist:
                if map_countries and (country_of_birth in country_table["de"].values or
                        country_of_birth in country_special.keys()):
                    if country_of_birth in country_table["de"].values:
                        country_en = country_table["en"][country_table["de"] == country_of_birth].values[0]
                    else:
                        country_en = country_special[country_of_birth]
                    try:
                        person.country_of_birth = cities.models.Country.objects.get(
                                name=country_en
                            )
                    except Country.DoesNotExist:
                        print("Warning: Did not find country of birth: {}".format(country_en))
                        warn += 1

                else:
                    print("Warning: Did not find country of birth: {}".format(country_of_birth))
                    warn += 1
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

        try:
            person.party = Party.objects.get(alt_names__contains=[biodata.find('PARTEI_KURZ').text])
        except Party.DoesNotExist:
            if biodata.find('PARTEI_KURZ').text == 'Plos':
                person.party = Party.objects.get(name="parteilos")
                pass
            else:
                print("Warning: Party not found: {}".format(biodata.find("PARTEI_KURZ").text))
                warn += 1

        person.active_country = Country.objects.get(name='Germany')
        person.positions = ['parliamentarian']
        person.information_source = "MDB Stammdata"
        person.save()

        wp_list = []

        # loop over wahlperioden
        for wp in mdb.findall('WAHLPERIODEN/WAHLPERIODE'):

            wp_list.append(int(wp.find('WP').text))

            # do not use seats of the Volkskammer
            if wp.find('MANDATSART').text == "Volkskammer":
                continue

            pp, created = ParlPeriod.objects.get_or_create(
                parliament=parl,
                n=wp.find('WP').text
                )

            seat, created = Seat.objects.get_or_create(
                parlperiod=pp,
                occupant=person
                )
            seat.start_date = german_date(wp.find('MDBWP_VON').text)
            seat.end_date = german_date(wp.find('MDBWP_BIS').text)

            # loop over institutions
            for ins in wp.findall('INSTITUTIONEN/INSTITUTION'):

                if ins.find('INSART_LANG').text == 'Fraktion/Gruppe':

                    try:
                        party = Party.objects.get(
                                alt_names__contains=[ins.find('INS_LANG').text]
                                )
                    except Party.DoesNotExist:
                        if ins.find('INS_LANG').text == "Fraktionslos":
                            person.party = Party.objects.get(name="fraktionslos")
                            pass

                        else:
                            print("Warning: Party not found: {}".format(ins.find('INS_LANG').text))
                            warn += 1

                else:
                    if verbosity > 0:
                        print("Other institution: {}".format(ins.find('INS_LANG').text))

            if wp.find('MANDATSART').text == "Direktwahl":
                seat.seat_type = Seat.DIRECT
                direct_region = wp.find('WKR_LAND').text
                try:
                    wk, created = Constituency.objects.get_or_create(
                        parliament=parl,
                        number=wp.find('WKR_NUMMER').text,
                        name=wp.find('WKR_NAME').text,
                        region=Region.objects.get(name=LANDS[direct_region])
                        )
                    wk.save()
                    seat.constituency = wk
                    seat.save()
                except KeyError:
                    print("Warning: Region of Direktmandat not found: {}".format(direct_region))
                    warn += 1
                except Region.DoesNotExist:
                    print("Warning: Region of Direktmandat not in regions: {}".format(direct_region))
                    warn += 1

            elif wp.find('MANDATSART').text == "Landesliste":
                seat.seat_type = Seat.LIST
                list_region = wp.find('LISTE').text
                try:
                    pl, created = PartyList.objects.get_or_create(
                        parlperiod=pp,
                        region=Region.objects.get(name=LANDS[list_region])
                        )
                    seat.list = pl
                    seat.save()
                    pl.save()
                except KeyError:
                    print("Warning: Region of Landesliste not found: {}".format(list_region))
                    warn += 1
                except Region.DoesNotExist:
                    print("Warning: Region of Landesliste not in regions: {}".format(list_region))
                    warn += 1

            else:
                print("Warning: Unknown Mandatsart: {}".format(wp.find('MANDATSART').text))

        person.in_parlperiod = wp_list
        person.save()

    print("Done. {} warnings.".format(warn))


if __name__ == '__main__':
    delete_all = True
    if delete_all:
        # to delete all existing entries for constituencies, partylists, seats and persons
        Constituency.objects.all().delete()
        PartyList.objects.all().delete()
        Party.objects.all().delete()
        Seat.objects.all().delete()
        Person.objects.all().delete()

    else:
        Person.objects.filter(information_source="MDB Stammdata").delete()

    # getting the data
    fetch_mdb_data()
    # updating the parties
    update_parties()
    # parsing the data
    parse_mdb_data(verbosity=0)

    add_party_colors = True

    if add_party_colors:
        pcolours = [
            {'party':'cducsu','colour':'#000000'},
            {'party':'spd','colour':'#EB001F'},
            {'party':'linke','colour':'#8C3473'},
            {'party':'fdp','colour':'#FFED00'},
            {'party':'afd','colour':'#cducsu'},
            {'party':'gruene','colour':'#64A12D'},
        ]
        for pc in pcolours:
            p, created = Party.objects.get_or_create(name=pc['party'])
            p.colour = pc['colour']
            p.save()



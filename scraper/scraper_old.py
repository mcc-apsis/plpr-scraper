# coding: utf-8

# old version of the scraper for a specific set of documents from the German Bundestag starting with the 17. period
# available at
# https://www.bundestag.de/ajax/filterlist/de/dokumente/protokolle/plenarprotokolle/plenarprotokolle/455046-455046/

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
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pprint
import platform

if platform.node() == "mcc-apsis":
    sys.path.append('/home/muef/tmv/BasicBrowser/')
    data_dir = '/home/muef/plpr-scraper/plenarprotokolle'
else:
    # local paths
    sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')
    data_dir = '/media/Data/MCC/plpr-scraper/data'

# imports and settings for django and database
# --------------------------------------------

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
# alternatively
#settings.configure(DEBUG=True)
django.setup()


from parliament.models import *
from parliament.tasks import *
from cities.models import *

log = logging.getLogger(__name__)

TXT_DIR = os.path.join(data_dir, 'txt')
OUT_DIR = os.path.join(data_dir, 'out')

DATE=re.compile('\w*,\s*(Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),\s*\w*\s*([0-9]*)\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember) ([0-9]{4})')
D_MONTHS = {
    'Januar':1,
    'Februar':2,
    'März':3,
    'April': 4,
    'Mai': 5,
    'Juni': 6,
    'Juli': 7,
    'August': 8,
    'September': 9,
    'Oktober': 10,
    'November': 11,
    'Dezember': 12
}

INDEX_URL = 'https://www.bundestag.de/plenarprotokolle'
ARCHIVE_URL = 'http://webarchiv.bundestag.de/archive/2013/0927/dokumente/protokolle/plenarprotokolle/plenarprotokolle/17%03.d.txt'

CHAIRS = ['Vizepräsidentin', 'Vizepräsident', 'Präsident', 'Präsidentin', 'Alterspräsident', 'Alterspräsidentin']

SPEAKER_STOPWORDS = ['ich zitiere', 'zitieren', 'Zitat', 'zitiert',
                     'ich rufe den', 'ich rufe die',
                     'wir kommen zur Frage', 'kommen wir zu Frage', 'bei Frage',
                     'fordert', 'fordern', u'Ich möchte',
                     'Darin steht', ' Aspekte ', ' Punkte ', 'Berichtszeitraum']

BEGIN_MARK = re.compile('Beginn:? [X\d]{1,2}.\d{1,2} Uhr')
END_MARK = re.compile('(\(Schluss:.\d{1,2}.\d{1,2}.Uhr\).*|\(*Schluss der Sitzung)|Die Sitzung ist geschlossen')


ANY_PARTY = re.compile('({})'.format('|'.join([x.pattern.strip() for x in PARTIES_REGEX.values()])))

# speaker types
PARTY_MEMBER = re.compile('\s*(.{5,140}\((.*)\)):\s*$')
PRESIDENT = re.compile('\s*((Alterspräsident(?:in)?|Vizepräsident(?:in)?|Präsident(?:in)?).{5,140}):\s*$')
STAATSSEKR = re.compile('\s*(.{5,140}, Parl\. Staatssekretär.*):\s*$')
STAATSMINISTER = re.compile('\s*(.{5,140}, Staatsminister.*):\s*$')
MINISTER = re.compile('\s*(.{5,140}, Bundesminister.*):\s*$')
WEHRBEAUFTRAGTER = re.compile('\s*(.{5,140}, Wehrbeauftragter.*):\s*$')
BUNDESKANZLER = re.compile('\s*(.{5,140}, Bundeskanzler.*):\s*$')
BEAUFTRAGT = re.compile('\s*(.{5,140}, Beauftragter? der Bundes.*):\s*$')
BERICHTERSTATTER = re.compile('\s*(.{4,140}?, Berichterstatter(in)?.*?):\s*')

PERSON_POSITION = ['Vizepräsident(in)?', 'Präsident(in)?',
                   'Alterspräsident(in)?', 'Bundeskanzler(in)?',
                   'Staatsminister(in)?', '(?<=,\s)Bundesminister(in)? (für)? .*$',
                   'Parl. Staatssekretär(in)?', '(?<=,\s)Berichterstatter(in)?']
PERSON_POSITION = re.compile(u'(%s)' % '|'.join(PERSON_POSITION), re.U)

NAME_REMOVE = [u'\\[.*\\]|\\(.*\\)', u' de[sr]', u'Ge ?genruf', 'Weiterer Zuruf', 'Zuruf', 'Weiterer',
               u', zur.*', u', auf die', u' an die', u', an .*', u'gewandt', 'Liedvortrag']
NAME_REMOVE = re.compile(u'(%s)' % '|'.join(NAME_REMOVE), re.U)

PERSON_PARTY = re.compile('\s*(.{4,140})\s\((.*)\)$')
TITLE = re.compile('[A-Z]?\.?\s*Dr.|Dr. h.c.| Prof. Dr.')

DEHYPHENATE = re.compile('(?<=[A-Za-z])(-\s*)\n', re.M)
INHYPHEN = re.compile(r'([a-z])-([a-z])', re.U)

TOP_MARK = re.compile('.*(?: rufe.*der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt)(.*)')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

ABG = 'Abg\.\s*(.*?)(\[[\wäöüßÄÖÜ /]*\])'

pp = pprint.PrettyPrinter(indent=4)

#db = os.environ.get('DATABASE_URI', 'sqlite:///data.sqlite')
#print("USING DATABASE {}".format(db))
#eng = dataset.connect(db)
#table = eng['de_bundestag_plpr']

# engine = create_engine(db)
# Base = declarative_base()
# Base.metadata.create_all(engine)
# Session = sessionmaker(bind=engine)
# db_session = Session()


# class Utterance(Base):
#     __tablename__ = "de_bundestag_plpr"
#
#     id = Column(Integer, primary_key=True)
#     wahlperiode = Column(Integer)
#     sitzung = Column(Integer)
#     sequence = Column(Integer)
#     speaker_cleaned = Column(String)
#     speaker_party = Column(String)
#     speaker = Column(String)
#     speaker_fp = Column(String)
#     type = Column(String)
#     text = Column(String)

class POI(object):
    def __init__(self,text):
        self.poitext = text
        self.speakers = []
        self.parties = ""
        self.type = None

        for m in re.findall(ABG,text):
            self.speakers.append(m[0].strip())
            text = text.replace(m[1],"")
        if ": " in text:
            sinfo = text.split(': ', 1)
            speaker = sinfo[0].split('[')
            if len(speaker) > 1:
                self.speakers.append(speaker[0].strip())
            else:
                self.parties = search_party_names(text)
            self.poitext = sinfo[1].strip()
            self.type = Interjection.SPEECH
        elif "Beifall" in text:
            self.parties = search_party_names(text)
            self.type = Interjection.APPLAUSE
        elif "Widerspruch" in text:
            self.parties = search_party_names(text)
            self.type = Interjection.OBJECTION
        elif "Lachen" in text or "Heiterkeit" in text:
            self.parties = search_party_names(text)
            self.type = Interjection.AMUSEMENT
        elif "Zuruf" in text:
            self.parties = search_party_names(text)
            self.type = Interjection.OUTCRY


class SpeechParser(object):


    def __init__(self, lines):
        self.lines = lines
        self.was_chair = True
        self.date = None

    def get_date(self):
        for line in self.lines:
            if DATE.match(line):
                d = int(DATE.match(line).group(2))
                m = int(D_MONTHS[DATE.match(line).group(3)])
                y = int(DATE.match(line).group(4))
                date = datetime.date(y,m,d)
                self.date = date
                return

    def parse_pois(self, group):

        for poi in group.split(' - '):
            poi_instance = POI(poi)
            yield (poi_instance)

    def __iter__(self):
        self.in_session = False
        self.chair = False
        self.text = []
        self.pars = []
        self.speaker = None
        self.speaker_party = None

        def emit():
            data = {
                'speaker': self.speaker,
                'speaker_party': self.speaker_party,
                'type': 'chair' if self.chair else 'speech',
                'pars': self.pars
            }
            self.was_chair = self.chair
            self.text = []
            self.pars = []
            return data

        def emit_poi(speaker, text):
            self.was_chair = False
            return {
                'speaker': speaker,
                'type': 'poi',
                'text': text
            }

        for line in self.lines:
            rline = line.strip()

            # Check if in session, session beginning, session ending
            if not self.in_session and BEGIN_MARK.match(line):
                self.in_session = True
                continue
            elif not self.in_session:
                continue

            if END_MARK.match(rline):
                return

            if not len(rline):
                continue

            is_top = False
            if TOP_MARK.match(line):
                is_top = True

            has_stopword = False
            for sw in SPEAKER_STOPWORDS:
                if sw.lower() in line.lower():
                    has_stopword = True

            nline = normalize(line)
            noparty = False
            speaker_match = (PRESIDENT.match(line) or
                             PARTY_MEMBER.match(line) or
                             STAATSSEKR.match(line) or
                             STAATSMINISTER.match(line) or
                             WEHRBEAUFTRAGTER.match(line) or
                             BUNDESKANZLER.match(line) or
                             BEAUFTRAGT.match(line) or
                             MINISTER.match(line))

            if PARTY_MEMBER.match(line):
                if not ANY_PARTY.match(normalize(PARTY_MEMBER.match(line).group(2))):
                    noparty=True
            if speaker_match is not None \
                    and not is_top \
                    and not noparty \
                    and not has_stopword:




                if self.speaker is None and self.text ==[] and self.pars==[]:
                    self.text = []
                else:
                    if len(self.pars) < 1:
                        par = {
                            'text': "\n\n".join(self.text).strip(),
                            'pois': []
                        }
                        self.pars.append(par)

                    yield emit()
                role = line.strip().split(' ')[0]
                self.speaker = speaker_match.group(1)
                self.speaker_party = search_party_names(line.strip().split(':')[0])
                self.chair = role in CHAIRS
                continue

            poi_match = POI_MARK.match(rline)
            if poi_match is not None:
                # if not poi_match.group(1).lower().strip().startswith('siehe'):
                #yield emit()
                par = {
                    'text': "\n\n".join(self.text).strip(),
                    'pois': []
                }
                #self.text = []
                for poi in self.parse_pois(poi_match.group(1)):
                    par['pois'].append(poi)
                self.pars.append(par)
                self.text = []
                continue

            self.text.append(rline)
        yield emit()


def file_metadata(filename):
    fname = os.path.basename(filename)
    try:
        return int(fname[:2]), int(fname[2:5])
    except:
        return int(fname[:2]), fname[2:5]


def parse_transcript(filename):
    wp, session = file_metadata(filename)
    try:
        with open(filename) as fh:
            content = fh.read()
            text = clean_text(content)
    except UnicodeDecodeError:
        print("Reloading in other encoding (windows-1252)")
        with open(filename, encoding="windows-1252") as fh:
            content = fh.read()
            text = clean_text(content)

    base_data = {
        'filename': filename,
        'sitzung': session,
        'wahlperiode': wp
    }
    print("Loading transcript: {}/{}, from {}".format(wp, session, filename))
    seq = 0
    parser = SpeechParser(text.split('\n'))
    parser.get_date()

    parl, created = Parl.objects.get_or_create(
        country=Country.objects.get(name="Germany"),
        level='N'
    )
    ps, created = ParlPeriod.objects.get_or_create(
        parliament=parl,
        n=wp
    )
    doc, created = Document.objects.get_or_create(
        parlperiod=ps,
        doc_type="Plenarprotokoll",
        date=parser.date,
        sitting=session
    )

    if not created:
        print("document already parsed to database")
        return 0

    doc.save()

    doc.utterance_set.all().delete()


    entries = []
    for contrib in parser:
        # update dictionary
        contrib.update(base_data)

        if contrib['speaker']:
            per = match_person_in_db(contrib['speaker'], wp)
        else:
            print("No speaker given, not saving the following contribution: {}".format(contrib))
            continue

        if per is None:
            print("Not able to match person, not saving the following contribution: {}".format(contrib))
            continue

        ut = Utterance(
            document=doc,
            speaker=per
        )
        ut.save()
        for par in contrib['pars']:
            para = Paragraph(
                utterance=ut,
                text=par['text'],
                word_count=len(par['text'].split()),
                char_len=len(par['text'])
            )
            para.save()
            for ij in par['pois']:
                if ij.type is None:
                    print(ij.poitext)
                    continue
                interjection = Interjection(
                    paragraph=para,
                    text=ij.poitext,
                    type=ij.type
                )
                interjection.save()

                if ij.parties:
                    for party_name in ij.parties.split(':'):
                        party, created = Party.objects.get_or_create(
                            name=party_name
                        )

                        interjection.parties.add(party)
                if ij.speakers:
                    for person in ij.speakers:
                        per = match_person_in_db(contrib['speaker'], wp)
                        if per is not None:
                            interjection.persons.add(per)

        contrib['sequence'] = seq

        contrib['speaker_fp'] = fingerprint(contrib['speaker'])
        contrib['speaker_party'] = search_party_names(contrib['speaker'])
        seq += 1
        entries.append(contrib)
    # db_session.bulk_insert_mappings(Utterance, entries)
    # db_session.commit()
    #print(entries)
    #write_db(entries)

    # q = '''SELECT * FROM de_bundestag_plpr WHERE wahlperiode = :w AND sitzung = :s
    #         ORDER BY sequence ASC'''
    # fcsv = os.path.basename(filename).replace('.txt', '.csv')
    # rp = eng.query(q, w=wp, s=session)
    # dataset.freeze(rp, filename=fcsv, prefix=OUT_DIR, format='csv')

def write_db(data):
    database = dataset.connect(db)
    table = database['plpr']
    for entry in data:
        table.insert(entry)

def clear_db():
    database = dataset.connect(db)
    table = database['plpr']
    table.delete()



def fetch_protokolle():
    for d in TXT_DIR, OUT_DIR:
        try:
            os.makedirs(d)
        except:
            pass

    urls = set()
    res = requests.get(INDEX_URL)
    doc = html.fromstring(res.content)
    for a in doc.findall('.//a'):
        url = urljoin(INDEX_URL, a.get('href'))
        if url.endswith('.txt'):
            urls.add(url)

    for i in range(30, 260):
        url = ARCHIVE_URL % i
        urls.add(url)

    new_urls = get_new_urls()
    urls = urls.union(new_urls)
    offset = 20
    while len(new_urls) == 20:
        new_urls = get_new_urls(offset)
        urls = urls.union(new_urls)
        offset += 20

    for url in urls:
        txt_file = os.path.join(TXT_DIR, os.path.basename(url))
        txt_file = txt_file.replace('-data', '')
        if os.path.exists(txt_file):
            continue

        r = requests.get(url)
        if r.status_code < 300:
            with open(txt_file, 'wb') as fh:
                fh.write(r.content)

            print(url, txt_file)


def get_new_urls(offset=0):
    base_url = "https://www.bundestag.de"
    url = "https://www.bundestag.de/ajax/filterlist/de/dokumente/protokolle/plenarprotokolle/plenarprotokolle/-/455046/"
    params = {"limit": 20,
              "noFilterSet": "true",
              "offset": offset
              }
    res = requests.get(url, params=params)
    doc = html.fromstring(res.content)
    return {urljoin(base_url, a.get('href')) for a in doc.findall('.//a')}


# ==========================================================================================================
# ==========================================================================================================


def match_person_in_db(name, wp):

    name = INHYPHEN.sub(r'\1\2', name)

    position = PERSON_POSITION.search(name)
    if position:
        print("=position: {}".format(position.group(0)))

        position = position.group(0)
    cname = PERSON_POSITION.sub('', name).strip(' ,')

    title = TITLE.match(cname)
    if title:
        title = title.group(0)
    cname = TITLE.sub('', cname).strip()

    party = PERSON_PARTY.match(cname)
    if party:
        party = party.group(2)
    cname = PERSON_PARTY.sub(r'\1', cname)

    ortszusatz = PERSON_PARTY.match(cname)
    if ortszusatz:
        ortszusatz = ortszusatz.group(2)
    cname = PERSON_PARTY.sub(r'\1', cname)

    cname = cname.strip('-').strip()  # remove beginning and tailing "-" and white space

    if len(cname.split(' ')) > 1:
        surname = cname.split(' ')[-1]
        firstname = cname.split(' ')[0]
    else:
        surname = cname
        firstname = ''

    query = Person.objects.filter(
        surname=surname,
        in_parlperiod__contains=[wp])

    if len(query) == 1:
        print("matched person in db: {}".format(name))
        return query.first()

    elif len(query) > 1:
        print("Found multiple persons in query")
        print(query)

        if firstname:
            query = query.filter(first_name=firstname)
            if len(query) == 1:
                print("ambiguity resolved")
                return query.first()

        if party:
            query = query.filter(party__alt_names__contains=[party])
            if len(query) == 1:
                print("ambiguity resolved")
                return query.first()

        # person, created = Person.objects.get_or_create(surname='Unmatched', first_name='Unmatched')

        print("Warning: Could not distinguish between persons!")
        print("name: {}".format(name))
        print("first name: {}, surname: {}".format(firstname, surname))
        print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
        return None

    else:
        print("Warning: person not found in database: {}".format(cname))
        print("name: {}".format(name))
        print("first name: {}, surname: {}".format(firstname, surname))
        print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
        print("query: {}".format(query))

        # person, created = Person.objects.get_or_create(surname='Unmatched', first_name='Unmatched')
        return None


if __name__ == '__main__':

    delete_all_entries = False

    if delete_all_entries:
        Document.objects.all().delete()
        Paragraph.objects.all().delete()
        Utterance.objects.all().delete()
        Interjection.objects.all().delete()

    pcolours = [
        {'party':'cducsu','colour':'#000000'},
        {'party':'spd','colour':'#EB001F'},
        {'party':'linke','colour':'#8C3473'},
        {'party':'fdp','colour':'#FFED00'},
        {'party':'afd','colour':'#0088FF'},
        {'party':'gruene','colour':'#64A12D'},
    ]
    for pc in pcolours:
        p, created = Party.objects.get_or_create(name=pc['party'])
        p.colour = pc['colour']
        p.save()

    # fetch_protokolle()

    for filename in os.listdir(TXT_DIR):
        # if "18064.txt" in filename:
        print("parsing {}".format(filename))
        parse_transcript(os.path.join(TXT_DIR, filename))
        #break

    #for s in Search.objects.all():
    #    do_search.delay(s.id)


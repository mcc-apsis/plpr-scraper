# coding: utf-8
from __future__ import print_function
import os, sys
import django
from django.conf import settings
import re
import requests
import dataset
import datetime
from xml.etree import ElementTree
from urllib.parse import urljoin
# Extract agenda numbers not part of normdatei
from normality import normalize
from normdatei.text import clean_text, clean_name, fingerprint  # , extract_agenda_numbers
from normdatei.parties import search_party_names, PARTIES_REGEX
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pprint
import platform
import zipfile

if platform.node() == "mcc-apsis":
    sys.path.append('/home/muef/tmv/BasicBrowser/')
    data_dir = '/home/muef/plpr-scraper/plenarprotokolle'
else:
    # local paths
    sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')
    data_dir = '/media/Data/MCC/Parliament Germany/Plenarprotokolle'

# imports and settings for django and database
# --------------------------------------------

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
# alternatively
#settings.configure(DEBUG=True)
django.setup()

# import from appended path
import parliament.models as pm
from parliament.tasks import do_search
import cities.models as cmodels

# regex notes:
# ? zero or one occurance
# * zero or more occurances
# + one or more occurances
# {n} n occurances
# {n, } n or more occurrances
# {n, m} n to m occurrances
# (?:x) -> non capturing group
# (?=x) lookahead
# (?<=x) lookbehind

# ? can also be used to change the default greedy behavior (take as many characters as possible) into a lazy one:
# e.g. (.*?) applied to (a) (b) will return (a), not (a) (b)

# \d -> numerical digit
# re.M is short for re.MULITLINE


DATE = re.compile('(?:Berlin|Bonn),\s*(Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),(?:\sden)?\s*(\d{1,2})\.\s*'
                  '(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember) (\d{4})')

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

BEGIN_MARK = re.compile('Beginn:? [X\d]{1,2}[.:]\d{1,2} Uhr|.*?Beginn:\s*\d{1,2}\s?[.,:]\s?\d{1,2}|Beginn:\s*\d{1,2} Uhr|'
                        'Beginn\s\d{1,2}\s*\.\s*\d{0,2}\sUhr|.*?Die Sitzung wird (um|urn) \d{1,2}[.:]*\d{0,2}\sUhr\s.*?(eröffnet|eingeleitet)')
INCOMPLETE_BEGIN_MARK = re.compile('.*?Die Sitzung wird um \d{1,2} Uhr|Beginn?:')

DISRUPTION_MARK = re.compile('^\s*Unterbrechung von [0-9.:]* bis [0-9.:]* Uhr|'
                             'Namensaufruf und Wahl')

END_MARK = re.compile('(\(Schluss:.\d{1,2}.\d{1,2}.Uhr\).*|\(*Schluss der Sitzung)|Die Sitzung ist geschlossen')

HEADER_MARK = re.compile('\d{0,6}\s*Deutscher Bundestag\s*[–\-]\s*\d{1,2}\.\s*Wahlperiode\s*[–\-]\s*\d{1,4}\. Sitzung.\s*(Bonn|Berlin),'
                         '|\s*\([A-Z]\)(\s*\([A-Z]\))*\s*$|\d{1,6}\s*$|^\s*\([A-Z]\)\s\)$|^\s*\([A-Z]$|^\s*[A-Z]\)$')

ANY_PARTY = re.compile('({})'.format('|'.join([x.pattern.strip() for x in PARTIES_REGEX.values()])))

# speaker type matches
PARTY_MEMBER = re.compile('\s*(.{4,140}?\(([^\(\)]*)\)):\s*')
PRESIDENT = re.compile('\s*((Alterspräsident(?:in)?|Vizepräsident(?:in)?|Präsident(?:in)?).{5,140}?):\s*')
STAATSSEKR = re.compile('\s*(.{4,140}?, Parl\. Staatssekretär.*?):\s*')
STAATSMINISTER = re.compile('\s*(.{4,140}?, Staatsminister.*?):\s*')
MINISTER = re.compile('\s*(.{4,140}?, Bundesminister.*?):\s*')
WEHRBEAUFTRAGTER = re.compile('\s*(.{4,140}?, Wehrbeauftragter.*?):\s*')
BUNDESKANZLER = re.compile('\s*(.{4,140}?, Bundeskanzler(in)?.*?):\s*')
BEAUFTRAGT = re.compile('\s*(.{4,140}?, Beauftragter? der Bundes.*):\s*')
BERICHTERSTATTER = re.compile('\s*(.{4,140}?, Berichterstatter(in)?.*?):\s*')

PERSON_POSITION = ['Vizepräsident(in)?', 'Präsident(in)?',
                   'Alterspräsident(in)?', 'Bundeskanzler(in)?',
                   'Staatsminister(in)?', '(?<=,\s)Bundesminister(in)?\s*(für)?.*$',
                   'Parl. Staatssekretär(in)?', '(?<=,\s)Berichterstatter(in)?', 'Abg.']
PERSON_POSITION = re.compile(u'(%s)' % '|'.join(PERSON_POSITION), re.U)

NAME_REMOVE = [u'\\[.*\\]|\\(.*\\)', u' de[sr]', u'Gegenruf', 'Weiterer Zuruf', 'Zuruf', 'Weiterer',
               u', zur.*', u', auf die', u' an die', u', an .*', u'gewandt', 'Liedvortrag']
NAME_REMOVE = re.compile(u'(%s)' % '|'.join(NAME_REMOVE), re.U)

PERSON_PARTY = re.compile('\s*(.{4,140})\s\((.*)\)$')
TITLE = re.compile('[A-Z]?\.?\s*Dr\.|Dr\.\sh\.c\.|Prof\.|Prof\.\sDr\.')

TOP_MARK = re.compile('.*(?: rufe.*der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt)(.*)')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
POI_BEGIN = re.compile('\(\s*[A-Z][^)]+$')
POI_END = re.compile('^[^(]+\)')

WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

ABG = 'Abg\.\s*(.*?)(\[[\wäöüßÄÖÜ /]*\])'
INHYPHEN = re.compile(r'([a-z])-([a-z])', re.U)

# ABBREVIATED = re.compile('[A-Z]\.')


pretty_printer = pprint.PrettyPrinter(indent=4)

# ============================================================
# write output to file and terminal

time_stamp = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
output_file = "./parlsessions_parser_output_" + time_stamp + ".log"


class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(output_file, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass


# interjections (poi = point of interjection?)
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
            self.type = pm.Interjection.SPEECH
        elif "Beifall" in text:
            self.parties = search_party_names(text)
            self.type = pm.Interjection.APPLAUSE
        elif "Widerspruch" in text:
            self.parties = search_party_names(text)
            self.type = pm.Interjection.OBJECTION
        elif "Heiterkeit" in text:
            self.parties = search_party_names(text)
            self.type = pm.Interjection.AMUSEMENT
        elif "Lachen" in text:
            self.parties = search_party_names(text)
            self.type = pm.Interjection.LAUGHTER
        elif "Zuruf" in text:
            self.parties = search_party_names(text)
            self.type = pm.Interjection.OUTCRY
        else:
            self.type = pm.Interjection.OTHER


class SpeechParser(object):

    def __init__(self, lines, verbosity=0):
        self.lines = lines
        self.line_number = 0
        self.was_chair = True
        self.date = None
        self.verbosity = verbosity
        self.in_session = False
        self.in_header = False
        self.in_poi = False
        self.poi_content = ""
        self.poi_linecounter = 0
        self.chair = False
        self.text = []
        self.pars = []
        self.speaker = None
        self.speaker_party = None
        self.warnings_counter = 0

    def get_date(self):
        for line in self.lines:
            date_match = DATE.search(line)
            if date_match:
                try:
                    d = int(date_match.group(2))
                    m = int(D_MONTHS[date_match.group(3)])
                    y = int(date_match.group(4))
                    date = datetime.date(y, m, d)
                    self.date = date
                    return

                except ValueError:
                    print("date from manuscript not readable: {}".format(DATE.match(line)))
                    print("group 1: {}".format(DATE.match(line).group(1)))
                    print("group 2: {}".format(DATE.match(line).group(2)))
                    print("group 3: {}".format(DATE.match(line).group(3)))
                    print("group 4: {}".format(DATE.match(line).group(4)))
                    raise ValueError

        print("Parser: Did not find date")
        return None

    def append_text_and_poi(self):
        par = {
            'text': dehyphenate(self.text),
            'pois': []
        }
        for poi_raw in self.poi_content.split(' - '):
            poi_obj = POI(poi_raw)
            par['pois'].append(poi_obj)
            if self.verbosity > 0:
                print("interjection: speakers: {}, party: {}, type: {},"
                      "\ninterjection text: {}".format(poi_obj.speakers, poi_obj.parties, poi_obj.type, poi_obj.poitext))

        self.pars.append(par)
        self.text = []
        self.poi_content = ""

    def emit(self):
        data = {
            'speaker': self.speaker,
            'speaker_party': self.speaker_party,
            'type': 'chair' if self.chair else 'speech',
            'pars': self.pars
        }
        self.was_chair = self.chair
        self.text = []
        self.pars = []
        if self.verbosity > 1:
            print("utterance: {}".format(data))
        return data

    def __iter__(self):

        for line in self.lines:
            self.line_number += 1
            line = line.strip()
            if verbosity > 1:
                print("- l{l:04d}: ".format(l=self.line_number) + line)

            # Check if in session, session beginning, session ending
            if not self.in_session and BEGIN_MARK.match(line):
                print("= matched begin mark: {}".format(line))
                self.in_session = True
                continue
            if not self.in_session and INCOMPLETE_BEGIN_MARK.match(line):
                print("! warning at line {}: Matched only incomplete begin mark: {}".format(self.line_number, line))
                self.warnings_counter += 1
                self.in_session = True

            elif not self.in_session:
                continue

            if DISRUPTION_MARK.match(line):
                continue

            if END_MARK.match(line):
                print("= matched end mark: {}".format(line))
                self.text.append(line)
                par = {
                    'text': dehyphenate(self.text),
                    # default for strip: removing leading and ending white space
                    'pois': []
                }
                self.pars.append(par)
                yield self.emit()
                return

            # empty line
            if not len(line):
                continue

            header_match = HEADER_MARK.match(line)
            if header_match is not None:
                if verbosity > 0:
                    print("= matched header: ", line)
                self.in_header = True
                continue

            if self.in_header and self.speaker is not None:
                if line.startswith(self.speaker):
                    if verbosity > 0:
                        print("= matched current speaker in header: {}".format(line))
                    continue
                else:
                    self.in_header = False

            is_top = False
            # new point on the agenda (top - tagesordnungspunkt)
            if TOP_MARK.match(line):
                if verbosity > 0:
                    print("= matched top mark: {}".format(line))
                is_top = True

            has_stopword = False
            for sw in SPEAKER_STOPWORDS:
                if sw.lower() in line.lower():
                    if verbosity > 0:
                        print("= setting stopword flag")
                    has_stopword = True

            noparty = False
            speaker_match = (PRESIDENT.match(line) or
                             PARTY_MEMBER.match(line) or
                             STAATSSEKR.match(line) or
                             STAATSMINISTER.match(line) or
                             WEHRBEAUFTRAGTER.match(line) or
                             BUNDESKANZLER.match(line) or
                             BEAUFTRAGT.match(line) or
                             MINISTER.match(line) or
                             BERICHTERSTATTER.match(line))

            if speaker_match is not None:
                if verbosity > 0:
                    print("= matched speaker at line {}: {}".format(self.line_number, speaker_match))
                self.in_poi = False

            if PARTY_MEMBER.match(line):
                if not ANY_PARTY.match(normalize(PARTY_MEMBER.match(line).group(2))):
                    if verbosity > 1:
                        print("= {} could not be identified".format(PARTY_MEMBER.match(line).group(2)))
                        print("= set noparty flag")
                    noparty = True

            if speaker_match is not None \
                    and not is_top \
                    and not noparty \
                    and not has_stopword:

                if self.speaker is None and self.text == [] and self.pars == []:
                    self.text = []
                else:
                    if verbosity > 1:
                        print("number of paragraphs in utterance: {}".format(len(self.pars)))
                    if len(self.pars) < 1:
                        par = {
                            'text': dehyphenate(self.text),
                            # default for strip: removing leading and ending white space
                            'pois': []
                        }
                        self.pars.append(par)

                    yield self.emit()

                role = line.strip().split(' ')[0]
                self.speaker = speaker_match.group(1)
                self.speaker_party = search_party_names(line.strip().split(':')[0])
                self.chair = role in CHAIRS
                continue

            poi_match = POI_MARK.match(line)
            if poi_match is not None:
                self.poi_content = poi_match.group(1)
                self.append_text_and_poi()
                continue

            if not self.in_poi:
                poi_begin = POI_BEGIN.match(line)
                if poi_begin:
                    if verbosity > 1:
                        print("= raised in_poi flag")
                    self.in_poi = True
                    self.poi_content = line
                    self.poi_linecounter = 0
                    continue
            else:
                self.poi_content += "\n" + line
                self.poi_linecounter += 1
                if POI_END.match(line):
                    self.poi_content = dehyphenate(self.poi_content).strip().strip('()')
                    self.append_text_and_poi()
                    if verbosity > 1:
                        print("= matched poi end")
                    self.in_poi = False
                if self.poi_linecounter > 10:
                    print("! Warning: No match of poi end after 10 lines. Going back to normal mode.")
                    self.warnings_counter += 1
                    self.in_poi = False
                    self.text.append(self.poi_content)
                continue

            self.text.append(line)

        print("Reached end of file")
        yield self.emit()


def file_metadata(filename):
    fname = os.path.basename(filename)
    try:
        return int(fname[:2]), int(fname[2:5])
    except:
        return int(fname[:2]), fname[2:5]


def german_date(str):
    if str is None:
        return None
    return datetime.datetime.strptime(str,"%d.%m.%Y").date()


def dehyphenate(text):
    DEHYPHENATE = re.compile('(?<=[A-Za-zäöüß])(-\s*)\n(?!\s*[A-ZÄÖÜ][a-zäöüß])', re.M)

    if isinstance(text, (list, tuple)):
        text = '\n'.join(text)
    text = DEHYPHENATE.sub('', text)
    return text.replace('\n', ' ')

# ====================================================================
# ========== parse function ==========================================
# ====================================================================

def parse_transcript(file, verbosity=1):

    warnings_counter2 = 0
    if isinstance(file, str):
        print("loading text from {}".format(file))
        try:
            with open(file) as fh:
                content = fh.read()
                text = clean_text(content)
        except UnicodeDecodeError:
            print("Reloading in other encoding (windows-1252)")
            with open(file, encoding="windows-1252") as fh:
                content = fh.read()
                text = clean_text(content)
        filename = file
        wp, session = file_metadata(filename)

    # open file in zip archive
    elif isinstance(file, zipfile.ZipExtFile):
        content = file.read()
        filename = file.name
        if filename.endswith(".xml"):
            root = ElementTree.fromstring(content)
            if verbosity > 0:
                print("loading text from {}".format(filename))

            # display contents of xml file
            if verbosity > 1:
                print("xml root: {}, attributes: {}".format(root.tag, root.attrib))
                for child in root:
                    print("xml child: {}, attributes: {}".format(child.tag, child.attrib))
                    print("xml beginning of text: {}".format(child.text[:100].replace('\n', ' ')))

            wp = root.find("WAHLPERIODE").text
            document_type = root.find("DOKUMENTART").text
            if document_type != "PLENARPROTOKOLL":
                print("Warning: document {} is not tagged as Plenarprotokoll but {}".format(filename, document_type))
                warnings_counter2 += 1
            number = root.find("NR").text
            session = number.split("/")[1]
            date = root.find("DATUM").text
            titel = root.find("TITEL").text
            text = clean_text(root.find("TEXT").text)
        else:
            print("filetype not xml")
            return 0

    else:
        print("invalid filetype")
        return 0

    base_data = {
        'filename': filename,
        'sitzung': session,
        'wahlperiode': wp
    }

    print("\nParsing transcript: {}/{}, from {}".format(wp, session, filename))
    utterance_counter = 0
    paragraph_counter = 0
    interjection_counter = 0

    # start parsing
    parser = SpeechParser(text.split('\n'), verbosity=verbosity)
    # get_date is not working for all documents
    parser.get_date()
    if isinstance(file, zipfile.ZipExtFile):
        if parser.date != german_date(date):
            print("! Warning: dates do not match")
            warnings_counter2 += 1
            print(parser.date)
            print(date)
            print(german_date(date))

    parl, created = pm.Parl.objects.get_or_create(
        country=cmodels.Country.objects.get(name="Germany"),
        level='N'
    )
    if created and verbosity > 0:
        print("created new object for parliament")

    pp, created = pm.ParlPeriod.objects.get_or_create(
                                    parliament=parl,
                                    n=wp)
    if created and verbosity > 0:
        print("created new object for legislative period")

    doc, created = pm.Document.objects.get_or_create(
        parlperiod=pp,
        doc_type="Plenarprotokoll",
        date=german_date(date)
    )
    if created:
        print("created new object for plenary session document")
    doc.sitting = session
    doc.save()

    doc.utterance_set.all().delete()

    # parser.__iter__ yields dict with paragraphs + speaker + speaker_party + interjections (poi)
    for contrib in parser:

        if verbosity > 1:
            print("saving utterance: {}".format(contrib))

        # update dictionary
        contrib.update(base_data)

        if contrib['speaker']:
            per = find_person_in_db(contrib['speaker'], wp, verbosity=verbosity)
        else:
            print("! Warning: No speaker given, not saving the following contribution: {}".format(contrib))
            warnings_counter2 += 1
            continue

        if per is None:
            print("! Warning: Not able to match person, not saving the following contribution: {}".format(contrib))
            warnings_counter2 += 1
            continue

        ut = pm.Utterance(
            document=doc,
            speaker=per
        )
        ut.save()
        utterance_counter += 1

        for par in contrib['pars']:

            if par['text']:
                para = pm.Paragraph(
                    utterance=ut,
                    text=par['text'],
                    word_count=len(par['text'].split()),
                    char_len=len(par['text'])
                )
                para.save()
                paragraph_counter += 1
            else:
                print("! Warning: Empty paragraph ({})".format(par))
                warnings_counter2 += 1
                for ij in par['pois']:
                    print("poi: {}".format(ij.poitext))
                continue

            for ij in par['pois']:
                if ij.type is None:
                    print("! Warning: Ommiting interjection. Interjection type not identified for: {}".format(ij.poitext))
                    warnings_counter2 += 1
                    continue
                interjection = pm.Interjection(
                    paragraph=para,
                    text=ij.poitext,
                    type=ij.type
                )
                interjection.save()
                interjection_counter += 1

                if ij.parties:
                    for party_name in ij.parties.split(':'):
                        party, created = pm.Party.objects.get_or_create(
                            name=party_name
                        )

                        interjection.parties.add(party)
                if ij.speakers:
                    for person in ij.speakers:
                        per = find_person_in_db(person, wp, verbosity=verbosity)
                        if per is not None:
                            interjection.persons.add(per)
                        else:
                            print("! Warning: Speaker could not be identified")
                            warnings_counter2 += 1

    if not parser.in_session:
        print("! Error: beginning of session not found")
        return (1, 0)

    print("==================================================")
    print("Summary for {}:".format(filename))
    print("number of utterances: {}".format(utterance_counter))
    print("number of paragraphs: {}".format(paragraph_counter))
    print("number of interjections: {}".format(interjection_counter))
    print("warnings in SpeechParser generator: {}".format(parser.warnings_counter))
    print("warnings in parse_transcript function: {}".format(warnings_counter2))
    print("==================================================")

    if utterance_counter <= 0:
        return (1, 0)
    else:
        return (0, parser.warnings_counter + warnings_counter2)

# ==========================================================================================================
# ==========================================================================================================


def find_person_in_db(name, wp, create=True, verbosity=1):

    name = INHYPHEN.sub(r'\1\2', name)
    name = NAME_REMOVE.sub('', name)

    position = PERSON_POSITION.search(name)
    if position and verbosity > 1:
        print("= position: {}".format(position.group(0)))

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

    cname = cname.strip('- ()')  # remove beginning and tailing "-", "(", ")" and white space

    if len(cname.split(' ')) > 1:
        surname = cname.split(' ')[-1]
        firstname = cname.split(' ')[0]
    else:
        surname = cname
        firstname = ''

    # find matching entry in database
    query = pm.Person.objects.filter(
        surname=surname,
        in_parlperiod__contains=[wp])

    if len(query) == 1:
        if verbosity > 1:
            print("= matched person in db: {}".format(name))
        return query.first()

    elif len(query) > 1:
        if verbosity > 1:
            print("= found multiple persons in query")
            print(query)

        if firstname:
            query = query.filter(first_name=firstname)
            if len(query) == 1:
                if verbosity > 1:
                    print("= ambiguity resolved")
                return query.first()

        if party:
            query = query.filter(party__alt_names__contains=[party])
            if len(query) == 1:
                if verbosity > 1:
                    print("ambiguity resolved")
                return query.first()

        print("! Warning: Could not distinguish between persons!")
        if verbosity > 0:
            print("name: {}".format(name))
            print("first name: {}, surname: {}".format(firstname, surname))
            print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))

        if create:
            person, created = pm.Person.objects.get_or_create(surname='Ambiguity', first_name='Ambiguity')
            return person
        else:
            return None

    else:
        try:
            person = pm.Person.objects.get(surname=surname,
                                           first_name=firstname)
            if title:
                person.title = title
            if party:
                try:
                    party_obj = pm.Party.objects.get(alt_names__contains=[party])
                    person.party = party_obj
                except pm.Party.DoesNotExist:
                    print("! Warning: party could not identified: {}".format(party))
            if ortszusatz:
                person.ortszusatz = ortszusatz
            person.save()
            return person

        except pm.Person.DoesNotExist:

            print("! Warning: person not found in database: {}".format(cname))
            if verbosity > 0:
                print("name: {}".format(name))
                print("first name: {}, surname: {}".format(firstname, surname))
                print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
                print("query: {}".format(query))

            if create:
                person = pm.Person(surname=surname, first_name=firstname)
                if title:
                    person.title = title
                if party:
                    try:
                        party_obj = pm.Party.objects.get(alt_names__contains=[party])
                        person.party = party_obj
                    except pm.Party.DoesNotExist:
                        print("! Warning: party could not identified: {}".format(party))
                if ortszusatz:
                    person.ortszusatz = ortszusatz
                # use position with data model "Post" ?

                person.save()
                print("Created person: {}".format(person))

                return person

            else:
                return None


def clear_db():
    database = dataset.connect(db)
    table = database['plpr']
    table.delete()

# =================================================================================================================
# =================================================================================================================


def lines_with_one_character(file):

    if isinstance(file, str):
        # print("loading text from {}".format(file))
        with open(file) as fh:
            text = fh.read()
        text = text.replace("\t", "").split("\n")

    # open file in zip archive
    elif isinstance(file, zipfile.ZipExtFile):
        content = file.read()
        filename = file.name
        if filename.endswith(".xml"):
            root = ElementTree.fromstring(content)
            text = root.find("TEXT").text.replace("\t", "").split("\n")
            # print("loading text from {}".format(filename))
        else:
            print("filetype not xml")
            return None
        file.close()

    text = [line.strip() for line in text if line.strip() != '']
    count = sum([1 for line in text if len(line) == 1])

    return count

# =================================================================================================================

# main execution script
if __name__ == '__main__':

    sys.stdout = Logger()

    # settings for parsing
    delete_protokolle = True
    delete_additional_persons = False
    delete_all = False
    verbosity = 0

    if delete_all:
        # pmodels.Person.objects.all().delete()
        # pmodels.Parl.objects.all().delete()
        # pmodels.ParlPeriod.objects.all().delete()
        pm.Interjection.objects.all().delete()
        pm.Paragraph.objects.all().delete()
        pm.Utterance.objects.all().delete()
        pm.Document.objects.all().delete()

    if delete_additional_persons:
        pm.Person.objects.filter(year_of_birth=None).delete()

    document_counter = 0
    count_errors = 0
    count_warnings_docs = 0
    count_warnings_sum = 0

    print("start parsing...")
    for wp in range(9, 5, -1):
        collection = "pp{wp:02d}-data.zip".format(wp=wp)
        print(collection)

        if delete_protokolle:
            pm.Interjection.objects.filter(paragraph_id__utterance_id__document_id__parlperiod_id__n=wp).delete()
            pm.Paragraph.objects.filter(utterance_id__document_id__parlperiod_id__n=wp).delete()
            pm.Utterance.objects.filter(document_id__parlperiod_id__n=wp).delete()
            pm.Document.objects.filter(parlperiod_id__n=wp).delete()

        archive = zipfile.ZipFile(os.path.join(data_dir, collection), 'r')
        print("loading files from {}".format(collection))
        filelist = archive.infolist()
        for zipitem in filelist:
            f = archive.open(zipitem)
            parser_errors, parser_warnings = parse_transcript(f, verbosity=verbosity)
            count_errors += parser_errors
            if parser_warnings > 0:
                count_warnings_docs += 1
                count_warnings_sum += parser_warnings

            document_counter += 1
            f.close()

            f = archive.open(zipitem)
            print("lines with one character: {}".format(lines_with_one_character(f)))
            print("==================================================\n")

            f.close()

        archive.close()


    print("\n==================================================")
    print("Summary for {} documents:".format(document_counter))
    print("Documents with errors: {}".format(count_errors))
    print("Documents with warnings: {}".format(count_warnings_docs))
    print("Sum of all warnings: {}".format(count_warnings_sum))


    #for s in Search.objects.all():
    #    do_search.delay(s.id)

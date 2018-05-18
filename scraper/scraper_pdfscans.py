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
# (?<=x) lookbehind

# ? can also be used to change the default greedy behavior (take as many characters as possible) into a lazy one:
# e.g. (.*?) applied to (a) (b) will return (a), not (a) (b)

# \d -> numerical digit
# re.M is short for re.MULITLINE


DATE = re.compile('\w*,\s*(Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),(?:\sden)?\s*(\d{1,2})\.\s*'
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

HEADER_MARK = re.compile('\d{1,6}?\s*Deutscher Bundestag\s*[–\-]\s*\d{1,2}\.\s*Wahlperiode\s*[–\-]\s*\d{1,4}\. Sitzung.\s*(Bonn|Berlin),'
                         '|\s*\([A-Z]\)(\s*\([A-Z]\))*\s*$|\d{1,6}\s*$|^\s*\([A-Z]\)\s\)$|^\s*\([A-Z]$|^\s*[A-Z]\)$')

ANY_PARTY = re.compile('({})'.format('|'.join([x.pattern.strip() for x in PARTIES_REGEX.values()])))

# speaker type matches
PARTY_MEMBER = re.compile('\s*(.{4,140}?\((.*)\)):\s*')
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
                   'Staatsminister(in)?', '(?<=,\s)Bundesminister(in)? (für)? .*$',
                   'Parl. Staatssekretär(in)?', '(?<=,\s)Berichterstatter(in)?']
PERSON_POSITION = re.compile(u'(%s)' % '|'.join(PERSON_POSITION), re.U)

NAME_REMOVE = [u'\\[.*\\]|\\(.*\\)', u' de[sr]', u'Ge ?genruf', 'Weiterer Zuruf', 'Zuruf', 'Weiterer',
               u', zur.*', u', auf die', u' an die', u', an .*', u'gewandt', 'Liedvortrag']
NAME_REMOVE = re.compile(u'(%s)' % '|'.join(NAME_REMOVE), re.U)

PERSON_PARTY = re.compile('\s*(.{4,140})\s\((.*)\)$')
TITLE = re.compile('[A-Z]?\.?\s*Dr.|Dr. h.c.| Prof. Dr.')

TOP_MARK = re.compile('.*(?: rufe.*der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt)(.*)')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

ABG = 'Abg\.\s*(.*?)(\[[\wäöüßÄÖÜ /]*\])'
DEHYPHENATE = re.compile('(?<=[A-Za-z])(-\s*)\n', re.M)
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


class SpeechParser(object):

    def __init__(self, lines, verbosity=0):
        self.lines = lines
        self.line_number = 0
        self.was_chair = True
        self.date = None
        self.verbosity = verbosity

    def get_date(self):
        for line in self.lines:
            if DATE.match(line):
                try:
                    d = int(DATE.match(line).group(2))
                    m = int(D_MONTHS[DATE.match(line).group(3)])
                    y = int(DATE.match(line).group(4))
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

    def parse_pois(self, group):

        for poi in group.split(' - '):
            poi_instance = POI(poi)
            yield (poi_instance)

    def __iter__(self):
        self.in_session = False
        self.in_header = False
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
            if self.verbosity > 1:
                print("utterance: {}".format(data))
            return data

        def emit_poi(speaker, text):
            self.was_chair = False
            return {
                'speaker': speaker,
                'type': 'poi',
                'text': text
            }

        for line in self.lines:
            self.line_number += 1
            if verbosity > 1:
                print("l: " + line)
            rline = line.strip()

            # Check if in session, session beginning, session ending
            if not self.in_session and BEGIN_MARK.match(line):
                print("Matched begin mark: {}".format(line))
                self.in_session = True
                continue
            if not self.in_session and INCOMPLETE_BEGIN_MARK.match(line):
                print("Warning at line {}: Matched only incomplete begin mark: {}".format(self.line_number, line))
                self.in_session = True

            elif not self.in_session:
                continue

            if DISRUPTION_MARK.match(rline):
                continue

            if END_MARK.match(rline):
                print("Matched end mark: {}".format(rline))
                return

            # empty line
            if not len(rline):
                if self.verbosity > 1:
                    print("Empty line")
                continue

            header_match = HEADER_MARK.match(line)
            if header_match is not None:
                print("matched header: ", line)
                self.in_header = True
                continue

            if self.in_header and self.speaker is not None:
                print("speaker: {}".format(self.speaker))
                if line.startswith(self.speaker):
                    print("header: {}".format(line))
                    continue
                else:
                    print(line)
                    self.in_header = False

            is_top = False
            # new point on the agenda (top - tagesordnungspunkt)
            if TOP_MARK.match(line):
                print("Matched top mark: {}".format(line))
                is_top = True

            has_stopword = False
            for sw in SPEAKER_STOPWORDS:
                if sw.lower() in line.lower():
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
                print("matched speaker at line {}: {}".format(self.line_number, speaker_match))

            if PARTY_MEMBER.match(line):
                if not ANY_PARTY.match(normalize(PARTY_MEMBER.match(line).group(2))):
                    noparty = True
            if speaker_match is not None \
                    and not is_top \
                    and not noparty \
                    and not has_stopword:

                if self.speaker is None and self.text == [] and self.pars == []:
                    self.text = []
                else:
                    if len(self.pars) < 1:
                        print(self.text)

                        text = '\n'.join(self.text)
                        text = DEHYPHENATE.sub('', text)
                        text = text.replace('\n', ' ')

                        print(text)

                        par = {
                            'text': text,
                            # default for strip: removing leading and ending white space
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
                par = {
                    'text': "\n".join(self.text).strip(),
                    'pois': []
                }
                for poi in self.parse_pois(poi_match.group(1)):
                    par['pois'].append(poi)
                    if self.verbosity > 0:
                        print("interjection: speakers: {}, party: {}, type: {},"
                              "\ninterjection text: {}".format(poi.speakers, poi.parties, poi.type, poi.poitext))

                self.pars.append(par)
                self.text = []

                continue
            self.text.append(rline)

        print("Reached end of file")
        # print(self.lines[:500])
        yield emit()


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


# ====================================================================
# ========== parse function ==========================================
# ====================================================================

def parse_transcript(file, verbosity=1):
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
        text = file.read()
        filename = file.name
        if filename.endswith(".xml"):
            root = ElementTree.fromstring(text)
            if verbosity > 0:
                print("loading text from {}".format(filename))

            # display contents of xml file
            if verbosity > 1:
                print("root: {}, attributes: {}".format(root.tag, root.attrib))
                for child in root:
                    print("child: {}, attributes: {}".format(child.tag, child.attrib))
                    print("beginning of text: {}".format(child.text[:100]))

            wp = root.find("WAHLPERIODE").text
            document_type = root.find("DOKUMENTART").text
            if document_type != "PLENARPROTOKOLL":
                print("Warning: document {} is not tagged as Plenarprotokoll but {}".format(filename, document_type))
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

    print("Parsing transcript: {}/{}, from {}".format(wp, session, filename))
    seq = 0
    utterance_counter = 0
    paragraph_counter = 0
    interjection_counter = 0

    # start parsing
    parser = SpeechParser(text.split('\n'), verbosity=verbosity)
    # get_date is not working for all documents
    parser.get_date()
    if isinstance(file, zipfile.ZipExtFile):
        if parser.date != german_date(date):
            print("Warning: dates do not match")
            print(parser.date)
            print(german_date(date))

    parl, created = pm.Parl.objects.get_or_create(
        country=cmodels.Country.objects.get(name="Germany"),
        level='N'
    )
    if created:
        print("created new object for parliament")

    pp, created = pm.ParlPeriod.objects.get_or_create(
        parliament=parl,
        n=wp
    )
    if created:
        print("created new object for legislative period")

    doc, created = pm.Document.objects.get_or_create(
        parlperiod=pp,
        doc_type="plenarprotokolle",
        date=german_date(date)
    )
    if created:
        print("created new object for plenary session document")
    doc.sitting = session
    doc.save()

    doc.utterance_set.all().delete()

    entries = []

    # parser.__iter__ yields dict with paragraphs + speaker + speaker_party + interjections (poi)
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
                print("Warning: Empty paragraph ({})".format(par))
                for ij in par['pois']:
                    print(ij.poitext)
                continue

            for ij in par['pois']:
                if ij.type is None:
                    print("interjection type not identified: {}".format(ij.poitext))
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
                        per = match_person_in_db(contrib['speaker'], wp)
                        if per is not None:
                            interjection.persons.add(per)

        contrib['sequence'] = seq

        contrib['speaker_fp'] = fingerprint(contrib['speaker'])
        contrib['speaker_party'] = search_party_names(contrib['speaker'])
        seq += 1
        entries.append(contrib)
        if verbosity > 0:
            print("contribution: {}".format(contrib))

    if not parser.in_session:
        print("Warning: beginning of session not found")
        return 1

    print("==================================================")
    print("parsing text from {} done.\nSummary:".format(filename))
    print("number of utterances: {}".format(utterance_counter))
    print("number of paragraphs: {}".format(paragraph_counter))
    print("number of interjections: {}".format(interjection_counter))
    print("==================================================\n")

    if utterance_counter <= 0:
        return 1
    else:
        return 0

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

    query = pm.Person.objects.filter(
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

        # person, created = pm.Person.objects.get_or_create(surname='Unmatched', first_name='Unmatched')

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

        # person, created = pm.Person.objects.get_or_create(surname='Unmatched', first_name='Unmatched')
        return None


def clear_db():
    database = dataset.connect(db)
    table = database['plpr']
    table.delete()

# =================================================================================================================
# =================================================================================================================


# main execution script
if __name__ == '__main__':

    sys.stdout = Logger()

    delete_protokolle = True
    add_party_colors = True

    if delete_protokolle:
        # pmodels.Parl.objects.all().delete()
        # pmodels.ParlPeriod.objects.all().delete()
        pm.Document.objects.all().delete()
        pm.Paragraph.objects.all().delete()
        pm.Utterance.objects.all().delete()
        pm.Interjection.objects.all().delete()
        # Person.objects.all().delete()

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
            p, created = pm.Party.objects.get_or_create(name=pc['party'])
            p.colour = pc['colour']
            p.save()

    verbosity = 1
    document_counter = 0
    count_errors = 0

    print("starting parsing")
    # for collection in os.listdir(data_dir):
    for collection in ['/media/Data/MCC/Parliament Germany/Plenarprotokolle/pp01-data.zip']:
        if collection.endswith(".zip"):
            archive = zipfile.ZipFile(os.path.join(data_dir, collection), 'r')
            print("loading files from {}".format(collection))
            filelist = archive.infolist()
            for zipitem in filelist[:10]:
                f = archive.open(zipitem)
                parser_result = parse_transcript(f, verbosity=verbosity)
                count_errors += parser_result
                document_counter += 1

            archive.close()
            # break

    print("Parsed {} documents.".format(document_counter))
    print("Documents with errors: {}".format(count_errors))

    #for s in Search.objects.all():
    #    do_search.delay(s.id)


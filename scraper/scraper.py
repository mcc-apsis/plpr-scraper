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
import parliament.models as pmodels
from parliament.tasks import do_search
import cities.models as cmodels

DATE = re.compile('\w*,\s*(Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),( den)*\s*(\d{1,2})\.\s*'
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

# \d -> numerical digit, {1,2} -> one or two times
END_MARK = re.compile('(\(Schluss:.\d{1,2}.\d{1,2}.Uhr\).*|\(*Schluss der Sitzung)|Die Sitzung ist geschlossen')

HEADER_MARK = re.compile('')

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

TOP_MARK = re.compile('.*(?: rufe.*der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt)(.*)')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
# re.M is short for re.MULITLINE
WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

ABG = 'Abg\.\s*(.*?)(\[[\wäöüßÄÖÜ /]*\])'

pp = pprint.PrettyPrinter(indent=4)

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


# interjections
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
            self.type = pmodels.Interjection.SPEECH
        elif "Beifall" in text:
            self.parties = search_party_names(text)
            self.type = pmodels.Interjection.APPLAUSE
        elif "Widerspruch" in text:
            self.parties = search_party_names(text)
            self.type = pmodels.Interjection.OBJECTION
        elif "Heiterkeit" in text:
            self.parties = search_party_names(text)
            self.type = pmodels.Interjection.AMUSEMENT
        elif "Lachen" in text:
            self.parties = search_party_names(text)
            self.type = pmodels.Interjection.LAUGHTER
        elif "Zuruf" in text:
            self.parties = search_party_names(text)
            self.type = pmodels.Interjection.OUTCRY


class SpeechParser(object):

    def __init__(self, lines):
        self.lines = lines
        self.was_chair = True
        self.date = None

    def get_date(self):
        for line in self.lines:
            if DATE.match(line):
                try:
                    d = int(DATE.match(line).group(3))
                    m = int(D_MONTHS[DATE.match(line).group(4)])
                    y = int(DATE.match(line).group(5))
                    date = datetime.date(y, m, d)
                    self.date = date
                    return

                except ValueError:
                    print("date from manuscript not readable: {}".format(DATE.match(line)))
                    print("group 1: {}".format(DATE.match(line).group(1)))
                    print("group 2: {}".format(DATE.match(line).group(2)))
                    print("group 3: {}".format(DATE.match(line).group(3)))
                    print("group 4: {}".format(DATE.match(line).group(4)))
                    print("group 5: {}".format(DATE.match(line).group(5)))
                    raise ValueError

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
            if verbosity > 1:
                print("l: " + line)
            rline = line.strip()

            # Check if in session, session beginning, session ending
            if not self.in_session and BEGIN_MARK.match(line):
                print("Matched begin mark: {}".format(line))
                self.in_session = True
                continue
            if not self.in_session and INCOMPLETE_BEGIN_MARK.match(line):
                print("Warning: Matched only incomplete begin mark: {}".format(line))
                self.in_session = True

            elif not self.in_session:
                continue

            if END_MARK.match(rline):
                print("Matched end mark: {}".format(rline))
                return

            if not len(rline):
                continue

            is_top = False
            if TOP_MARK.match(line):
                print("matched top mark: {}".format(line))
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
                             MINISTER.match(line))

            if PARTY_MEMBER.match(line):
                if not ANY_PARTY.match(normalize(PARTY_MEMBER.match(line).group(2))):
                    noparty = True
            if speaker_match is not None \
                    and not is_top \
                    and not noparty \
                    and not has_stopword:

                if self.speaker is None and self.text ==[] and self.pars==[]:
                    self.text = []
                else:
                    if len(self.pars) < 1:
                        par = {
                            'text': "\n".join(self.text).strip(),
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
                    print("interjection: speakers: {}, party: {}, type: {},"
                          "\ntext: {}".format(poi.speakers, poi.parties, poi.type, poi.poitext))

                self.pars.append(par)
                self.text = []

                continue
            self.text.append(rline)

        print("reached end of file\nBeginning of document:")
        print(self.lines[:500])
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
            text = root.find("TEXT").text
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
    parser = SpeechParser(text.split('\n'))
    # get_date is not working for all documents
    parser.get_date()
    if isinstance(file, zipfile.ZipExtFile):
        if parser.date != german_date(date):
            print("Warning: dates do not match")
            print(parser.date)
            print(german_date(date))

    parl, created = pmodels.Parl.objects.get_or_create(
        country=cmodels.Country.objects.get(name="Germany"),
        level='N'
    )
    ps, created = pmodels.ParlSession.objects.get_or_create(
        parliament=parl,
        n=wp
    )
    doc, created = pmodels.Document.objects.get_or_create(
        parlsession=ps,
        doc_type="plenarprotokolle",
        date=german_date(date)
    )
    doc.sitting = session
    doc.save()

    doc.utterance_set.all().delete()

    entries = []

    # parser.__iter__ yields dict with paragraphs + speaker + speaker_party
    for contrib in parser:

        # update dictionary
        contrib.update(base_data)
        sc = clean_name(contrib['speaker'])

        # fingerprint function from normdatei.text
        fp = fingerprint(sc)

        if fp is not None:
            per, created = pmodels.Person.objects.get_or_create(
                surname=fp.split('-')[-1],
                first_name=fp.split('-')[0],
            )
            per.clean_name = sc
            if created:
                print("created person: {}".format(sc))
            per.save()

            if per.party is None:
                try:
                    per.party = pmodels.Party.objects.get(name=contrib['speaker_party'])
                    per.save()
                except pmodels.Party.DoesNotExist:
                    print("Warning: Did not find party: {}".format(contrib['speaker_party']))
                    pass
        else:
            print("fingerprint of speaker is None, not saving the following contribution: {}".format(contrib))
            continue

        ut = pmodels.Utterance(
            document=doc,
            speaker=per
        )
        ut.save()
        utterance_counter += 1

        for par in contrib['pars']:
            para = pmodels.Paragraph(
                utterance=ut,
                text=par['text'],
                word_count=len(par['text'].split()),
                char_len=len(par['text'])
            )
            para.save()
            paragraph_counter += 1

            for ij in par['pois']:
                if ij.type is None:
                    print("interjection type not identified: {}".format(ij.poitext))
                    continue
                interjection = pmodels.Interjection(
                    paragraph=para,
                    text=ij.poitext,
                    type=ij.type
                )
                interjection.save()
                interjection_counter += 1

                if ij.parties:
                    for party_name in ij.parties.split(':'):
                        party, created = pmodels.Party.objects.get_or_create(
                            name=party_name
                        )

                        interjection.parties.add(party)
                if ij.speakers:
                    for person in ij.speakers:
                        sc = clean_name(person)
                        fp = fingerprint(sc)
                        if fp is not None:
                            per, created = pmodels.Person.objects.get_or_create(
                                surname=fp.split('-')[-1],
                                first_name=fp.split('-')[0],
                            )
                            per.clean_name = sc
                            if created:
                                print("created person: {}".format(sc))
                            per.save()
                            interjection.persons.add(per)

        contrib['sequence'] = seq

        contrib['speaker_fp'] = fingerprint(sc)
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

    return 0


def clear_db():
    database = dataset.connect(db)
    table = database['plpr']
    table.delete()


# main execution script
if __name__ == '__main__':

    sys.stdout = Logger()

    delete_protokolle = True
    add_party_colors = True

    if delete_protokolle:
        # pmodels.Parl.objects.all().delete()
        pmodels.ParlSession.objects.all().delete()
        pmodels.Document.objects.all().delete()
        pmodels.Paragraph.objects.all().delete()
        pmodels.Utterance.objects.all().delete()
        pmodels.Interjection.objects.all().delete()
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
            p, created = pmodels.Party.objects.get_or_create(name=pc['party'])
            p.colour = pc['colour']
            p.save()

    verbosity = 1
    document_counter = 0
    count_errors = 0

    print("starting parsing")
    for collection in os.listdir(data_dir):
        if collection.endswith(".zip"):
            archive = zipfile.ZipFile(os.path.join(data_dir, collection), 'r')
            print("loading files from {}".format(collection))
            filelist = archive.infolist()
            for zipitem in filelist[:2]:
                f = archive.open(zipitem)
                parser_result = parse_transcript(f, verbosity=verbosity)
                count_errors += parser_result
                document_counter += 1

            archive.close()

    print("Parsed {} documents.".format(document_counter))
    print("Documents with errors: {}".format(count_errors))

    #for s in Search.objects.all():
    #    do_search.delay(s.id)


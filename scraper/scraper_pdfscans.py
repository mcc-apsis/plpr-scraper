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
from normdatei.text import fingerprint  # , extract_agenda_numbers
from normdatei.parties import search_party_names
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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

from parsing_utils import dehyphenate, find_person_in_db, POI, clean_text, correct_pdf_parsing_errors
from regular_expressions_global import *

import pprint
pretty_printer = pprint.PrettyPrinter(indent=4)

# ============================================================
# write output to file and terminal

time_stamp = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
output_file = "./parlsessions_pdf_parser_output_" + time_stamp + ".log"
print("log file: {}".format(output_file))


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
        for poi_raw in re.split(' [-–—]-? ', self.poi_content):
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

        no_lines = len(self.lines)
        self.line_number = -1

        while self.line_number + 1 < no_lines:
            self.line_number += 1
            line = self.lines[self.line_number].strip()
            if verbosity > 1:
                print("- l{l:04d}: ".format(l=self.line_number) + line)

            # Check if in session, session beginning, session ending
            if not self.in_session and BEGIN_MARK.match(line):
                print("= matched begin mark at line {}: {}".format(self.line_number, line))
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

            for k in range(1,3):
                lines = "\n".join(self.lines[self.line_number:self.line_number+k])
                lines = dehyphenate(lines)
                if END_MARK.search(lines):
                    print("= matched end mark at line {}: {}".format(self.line_number, lines))
                    self.text.append(lines)
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

            # match repeated mentioning of speaker from header
            if self.speaker:
                speaker = self.speaker.replace('Dr. ', '')
                speaker = speaker.split(" (")[0]
                speaker = speaker.split(", ")[0]
                # print("looking for {}".format(speaker))
                try:
                    SPEAKER_HEADER = re.compile('.{0,30}%s' % speaker)
                    if SPEAKER_HEADER.match(line):
                        if verbosity > 0:
                            print("= matched speaker in header: ", line)
                        continue
                except:
                    pass

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

            for k in range(1,4):
                lines = "\n".join(self.lines[self.line_number:self.line_number+k])
                lines = dehyphenate(lines, nl=True)
                # print(repr(lines)) # print with escape characters
                speaker_match = (PRESIDENT.match(lines) or
                                 PARTY_MEMBER.match(lines) or
                                 STAATSSEKR.match(lines) or
                                 STAATSMINISTER.match(lines) or
                                 WEHRBEAUFTRAGTER.match(lines) or
                                 BUNDESKANZLER.match(lines) or
                                 BEAUFTRAGT.match(lines) or
                                 MINISTER.match(lines) or
                                 BERICHTERSTATTER.match(lines))

                if speaker_match is not None:
                    if verbosity > 0:
                        print("= matched speaker at line {}: {}".format(self.line_number, speaker_match))
                    self.in_poi = False
                    self.line_number += k - 1
                    break

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

        print("! Warning: Reached end of file without end mark")
        self.warnings_counter += 1
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

            wp = int(root.find("WAHLPERIODE").text)
            document_type = root.find("DOKUMENTART").text
            if document_type != "PLENARPROTOKOLL":
                print("Warning: document {} is not tagged as Plenarprotokoll but {}".format(filename, document_type))
                warnings_counter2 += 1
            number = root.find("NR").text
            session = int(number.split("/")[1])
            date = root.find("DATUM").text
            titel = root.find("TITEL").text
            text = clean_text(root.find("TEXT").text)
            text = correct_pdf_parsing_errors(text)
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
        date=german_date(date),
        sitting=session,
        text_source="from https://www.bundestag.de/service/opendata (scans of pdfs with xml metadata)"
    )
    if created:
        print("created new object for plenary session document")
    doc.save()

    doc.utterance_set.all().delete()

    # parser.__iter__ yields dict with paragraphs + speaker + speaker_party + interjections (poi)
    for contrib in parser:

        if verbosity > 1:
            print("saving utterance: {}".format(contrib))

        # update dictionary
        contrib.update(base_data)

        if contrib['speaker']:
            info_dict = {'wp': wp, 'session': session, 'source_type': 'PDF/SP'}
            per = find_person_in_db(contrib['speaker'], add_info=info_dict, verbosity=verbosity)
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
                        info_dict = {'wp': wp, 'session': session, 'source_type': 'PDF/POI'}
                        per = find_person_in_db(person, add_info=info_dict, verbosity=verbosity)
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
    delete_protokolle = False
    delete_additional_persons = False
    delete_all = False
    verbosity = 0

    if delete_all:
        print("Deleting all documents, utterances, paragraphs and interjections.")
        # pmodels.Person.objects.all().delete()
        # pmodels.Parl.objects.all().delete()
        # pmodels.ParlPeriod.objects.all().delete()
        pm.Interjection.objects.all().delete()
        pm.Paragraph.objects.all().delete()
        pm.Utterance.objects.all().delete()
        pm.Document.objects.all().delete()
        print("Deletion done.")

    if delete_additional_persons:
        print("Deleting all persons added from protocol parsing.")
        pm.Person.objects.filter(information_source__startswith="from protocol scraping").delete()

    document_counter = 0
    count_errors = 0
    count_warnings_docs = 0
    count_warnings_sum = 0

    wps = range(16, 12, -1)
    sessions = range(1, 300)

    print("start parsing...")
    for wp in wps:
        collection = "pp{wp:02d}-data.zip".format(wp=wp)
        print(collection)

        if delete_protokolle:
            print("Deleting all documents, utterances, paragraphs and interjections from wp {}".format(wp))
            pm.Interjection.objects.filter(paragraph_id__utterance_id__document_id__parlperiod_id__n=wp).delete()
            pm.Paragraph.objects.filter(utterance_id__document_id__parlperiod_id__n=wp).delete()
            pm.Utterance.objects.filter(document_id__parlperiod_id__n=wp).delete()
            pm.Document.objects.filter(parlperiod_id__n=wp).delete()

        archive = zipfile.ZipFile(os.path.join(data_dir, collection), 'r')
        print("loading files from {}".format(collection))
        filelist = [fzip.filename for fzip in archive.infolist()]

        for session in sessions:
            filename = "{wp:02d}{s:03d}.xml".format(wp=wp, s=session)
            if filename in filelist:

                f = archive.open(filename)
                print(f)
                parser_errors, parser_warnings = parse_transcript(f, verbosity=verbosity)
                count_errors += parser_errors
                if parser_warnings > 0:
                    count_warnings_docs += 1
                    count_warnings_sum += parser_warnings

                document_counter += 1
                f.close()

                f = archive.open(filename)
                print("lines with one character: {}".format(lines_with_one_character(f)))
                print("==================================================\n")

                f.close()
            else:
                print("{} not in archive".format(filename))

        archive.close()


    print("\n==================================================")
    print("Summary for {} documents:".format(document_counter))
    print("Documents with errors: {}".format(count_errors))
    print("Documents with warnings: {}".format(count_warnings_docs))
    print("Sum of all warnings: {}".format(count_warnings_sum))


    #for s in Search.objects.all():
    #    do_search.delay(s.id)

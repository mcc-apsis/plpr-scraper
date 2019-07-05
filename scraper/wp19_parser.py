#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import lxml.etree as etree
import datetime as dt

import django
import platform

if platform.node() == "srv-mcc-apsis":
    sys.path.append('/home/leey/tmv/BasicBrowser/')
    xml_path = "/home/leey/plpr-scraper/data/19wahlperiode/"
elif platform.node() == 'finn-ThinkPadMCC':
    # local paths
    sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')
    xml_path = '/media/Data/MCC/Parliamentary_protocols/plpr-scraper/data/19wahlperiode/'
else:
    # local paths
    sys.path.append('/home/leey/Documents/Data/tmv/BasicBrowser/')
    xml_path = "/home/leey/Documents/Data/plpr-scraper/data/19wahlperiode/"

#sys.path.append('/home/galm/software/django/tmv/BasicBrowser/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
django.setup()

# import from appended path
import parliament.models as pm
import cities.models as cmodels

from parsing_utils import find_person_in_db, POI, dehyphenate_with_space, clean_text
from regular_expressions_global import POI_MARK
# ============================================================
# write output to file and terminal

import pprint
pretty_printer = pprint.PrettyPrinter(indent=4)

time_stamp = dt.datetime.now().strftime("%y%m%d_%H%M%S")
output_file = "./parlsessions_bundestag_parser_output_" + time_stamp + ".log"
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


class parse_xml_items(object):

    def __init__(self, xtree, v=1, period=None, session=None):
        self.v = v
        self.divs = xtree.findall("//tagesordnungspunkt")

        self.wp = int(xtree.xpath('vorspann/kopfdaten/plenarprotokoll-nummer/wahlperiode/text()')[0])
        self.session = int(xtree.xpath('vorspann/kopfdaten/plenarprotokoll-nummer/sitzungsnr/text()')[0])

        if period is not None:
            if period != self.wp:
                print("! Warning: period number not matching: {} {}".format(period, self.wp))

        if session is not None:
            if session != self.session:
                print("! Warning: session number not matching: {} {}".format(session, self.session))

        # cannot get date from kopfdaten, so used info from dbtplenarprotokoll instead
        self.date = dt.datetime.strptime(xtree.xpath("//dbtplenarprotokoll/@sitzung-datum")[0], '%d.%m.%Y')

        self.original_source = "www.bundestag.de/service/opendata"

        if self.v > 0:
            print("xml with protocol {}/{} from {}".format(self.wp, self.session, self.date))

    def get_or_create_objects(self):

        replace_old_documents = False

        parl, created = pm.Parl.objects.get_or_create(
            country=cmodels.Country.objects.get(name="Germany"),
            level='N')
        if created and self.v > 0:
            print("created new object for parliament")

        pp, created = pm.ParlPeriod.objects.get_or_create(
            parliament=parl,
            n=self.wp)
        if created and self.v > 0:
            print("created new object for legislative period")

        if replace_old_documents == True:
            doc, created = pm.Document.objects.get_or_create(
                parlperiod=pp,
                doc_type="Plenarprotokoll",
                date=self.date
            )
            if created:
                print("created new object for plenary session document")
        else:
            doc = pm.Document(
                parlperiod=pp,
                doc_type="Plenarprotokoll",
                date=self.date
            )

        doc.sitting = self.session
        doc.text_source = "XML from " + self.original_source
        doc.save()

        # delete old utterances associated with the doc
        doc.utterance_set.all().delete()
        self.doc = doc
        return doc

    def create_paragraph(self, text, utterance):
        text = "\n".join(text).replace("\n\n", "\n")
        text = clean_text(text)
        para = pm.Paragraph(
            utterance=utterance,
            text=text,
            word_count=len(text.split()),
            char_len=len(text)
        )
        para.save()
        return para

    def add_interjections(self, text, paragraph):
        poi_match = POI_MARK.match(text)
        if poi_match is not None:
            self.poi_content = poi_match.group(1)

        for poi_raw in re.split('\s[-–]-?\.?\s', self.poi_content):
            # de-hyphenate:
            poi_raw = dehyphenate_with_space(poi_raw)
            poi_obj = POI(poi_raw)
            if self.v > 1:
                print("interjection: speakers:  {}, party: {}, type: {},"
                      "\ninterjection text: {}".format(poi_obj.speakers, poi_obj.parties,
                                                       poi_obj.type, poi_obj.poitext))

            interjection = pm.Interjection(
                paragraph=paragraph,
                text=poi_obj.poitext,
                type=poi_obj.type
            )
            interjection.save()

            if poi_obj.parties:
                for party_name in poi_obj.parties.split(':'):
                    party, created = pm.Party.objects.get_or_create(
                        name=party_name
                    )
                    interjection.parties.add(party)

            if poi_obj.speakers:
                for person in poi_obj.speakers:
                    per = find_person_in_db(person, add_info={'wp': self.wp, 'session': self.session,
                                                              'source_type': 'Bundestag XML'}, verbosity=self.v)
                    if per is not None:
                        interjection.persons.add(per)
                    else:
                        print("! Warning: Speaker could not be identified")

    def run(self):

        self.get_or_create_objects()

        text = []

        ### start parsing of speeches
        speech_list = ['J','O','J_1']
        for div in self.divs: # speech in list of <rede>
            if self.v > 1:
                print("speech id: {}".format(div.get("top-id")))

            # finding agenda item
            for top in div.xpath('child::p'):
                if top.get('klasse') == "T_NaS":
                    agenda_item, created = pm.AgendaItem.objects.get_or_create(
                    title = top.text,
                    document = self.doc
                    )

            for sp in div:
                if sp.tag == "rede": # finding <rede> in <tagesordnungspunkt>
                    for uts in sp: # child elements of <rede>
                        if uts.tag == "p" and uts.get("klasse") == "redner" or uts.tag =="name":
                            if uts.tag == "p": # extracting speaker information
                                vorname = uts.xpath('redner/name/vorname/text()')
                                nachname = uts.xpath('redner/name/nachname/text()')
                                fullname = vorname + nachname

                                if len(fullname) > 1:
                                    fullname = fullname[0] + ' ' + fullname[1]

                                # match speaker to database:
                                info_dict = {}
                                # for nameidxp in uts.xpath('talker/name.id/text()'): info_dict['nameid'] = nameidxp
                                for partyxp in uts.xpath('redner/name/fraktion/text()'): info_dict['party'] = partyxp
                                for rolexp in uts.xpath('redner/name/rolle_lang/text()'): info_dict['role'] = rolexp
                                info_dict['wp'] = self.wp
                                info_dict['session'] = self.session
                                info_dict['source_type'] = 'Bundestag XML'

                                if fullname != []:
                                    speaker = find_person_in_db(fullname, add_info=info_dict, verbosity=self.v)

                                if speaker is None:
                                    print(fullname)

                                # complete last utterance if there is still text
                                if text:
                                    para = self.create_paragraph(text, ut)

                                # create new utterance
                                text = []

                                ut = pm.Utterance(
                                    document = self.doc,
                                    speaker = speaker,
                                    agenda_item = agenda_item
                                    #speaker_role=speaker_role
                                )
                                ut.save()


                            elif uts.tag == "name":
                                fullname = uts.text.strip(':')
                                speaker = find_person_in_db(fullname, verbosity=self.v)

                #speaker_role_set = pm.SpeakerRole.objects.filter(alt_names__contains=[rolexp])
                #if len(speaker_role_set) < 1:
                #    speaker_role = pm.SpeakerRole(name=rolexp, alt_names=[rolexp])
                #    speaker_role.save()
                #else:
                #    speaker_role = speaker_role_set.first()
                #    if len(speaker_role_set) > 1:
                #       print("Warning: several speaker roles matching")

                                # complete last utterance if there is still text
                                if text:
                                    para = self.create_paragraph(text, ut)

                                # create new utterance
                                text = []

                                ut = pm.Utterance(
                                    document=self.doc,
                                    speaker=speaker,
                                    agenda_item = agenda_item
                                    #speaker_role=speaker_role
                                )
                                ut.save()
                                if self.v > 1:
                                    print("tag = name", speaker)

                        elif uts.tag == "p" and uts.get("klasse") in speech_list:
                            if uts.text:
                                text.append(uts.text)
                            if self.v > 1:
                                print("tag = p", uts.text)

                        elif uts.tag == "kommentar":
                            if text:
                                para = self.create_paragraph(text, ut)
                                text = []
                            self.add_interjections(uts.text, para)
                            if self.v > 1:
                                print("tag = kommentar", uts.text)
                        #else:
                        #    print("unknown tag")

                    if text:
                        para = self.create_paragraph(text, ut)


# =================================================================================================================

# main execution script
if __name__ == '__main__':

    sys.stdout = Logger()

    single_doc = True
    replace_docs = False

    delete_all = False
    delete_additional_persons = False

    verbosity = 1

    if delete_all:
        print("Deleting all documents, utterances, paragraphs and interjections.")
        pm.Interjection.objects.all().delete()
        pm.Paragraph.objects.all().delete()
        pm.Utterance.objects.all().delete()
        pm.Document.objects.all().delete()
        print("Deletion done.")
    if delete_additional_persons:
        print("Deleting all persons added from protocol parsing.")
        pm.Person.objects.filter(information_source__startswith="from protocol scraping").delete()

    if single_doc:
        # single file
        wp = 19
        session = 64
        #{wp:02d}/
        xml_file = os.path.join(xml_path, "{wp:02d}{sn:03d}-data.xml".format(wp=wp, sn=session))
        namespaces = {'tei': 'http://www.tei-c.org/ns/1.0'}

        print("reading from {}".format(xml_file))

        xtree = etree.parse(xml_file)
        parser = parse_xml_items(xtree, v=verbosity)

        #pm.Document.objects.filter(parlperiod__n=parser.wp, sitting=parser.session).delete()
        parser.run()
        print("Done.")

        exit()

    # go through all scripts iteratively
    pperiod = 19
    for session in range(86, 300):

        xml_file = os.path.join(xml_path, "{wp:02d}{sn:03d}-data.xml".format(wp=pperiod, sn=session))

        if os.path.isfile(xml_file):
            print("reading from {}".format(xml_file))

            xtree = etree.parse(xml_file)
            if replace_docs:
                pm.Document.objects.filter(parlperiod__n=pperiod, sitting=session).delete()
            pm.Document.objects.filter(parlperiod__n=pperiod, sitting=session,
                                       text_source__startswith="from protocol scraping").delete()

            parser = parse_xml_items(xtree, period=pperiod, session=session, v=verbosity)
            parser.run()

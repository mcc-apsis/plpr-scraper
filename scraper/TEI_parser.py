import os
import difflib
import zipfile
# from xml.etree import ElementTree
import lxml.etree as etree
import random
import re
import sys
import datetime

import django
import platform

if platform.node() == "mcc-apsis":
    sys.path.append('/home/muef/tmv/BasicBrowser/')
else:
    # local paths
    sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')

#sys.path.append('/home/galm/software/django/tmv/BasicBrowser/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
django.setup()

# import from appended path
import parliament.models as pm
from parliament.tasks import do_search, run_tm
import cities.models as cmodels
from django.contrib.auth.models import User
import tmv_app.models as tm

from parsing_utils import find_person_in_db, POI, dehyphenate_with_space, clean_text
from regular_expressions_global import POI_MARK

# ============================================================
# write output to file and terminal

import pprint
pretty_printer = pprint.PrettyPrinter(indent=4)

time_stamp = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
output_file = "./parlsessions_tei_parser_output_" + time_stamp + ".log"
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


class parse_tei_items(object):

    def __init__(self, xtree, v=1, period=None, session=None):
        self.v = v
        self.divs = xtree.findall("//body//div")
        self.wp = int(xtree.xpath("//legislativePeriod//text()")[0])
        self.session = int(re.findall(r'\b\d+\b', xtree.xpath("//sessionNo//text()")[0])[0])
        if period is not None:
            if period != self.wp:
                print("! Warning: period number not matching: {} {}".format(period, self.wp))

        if session is not None:
            if session != self.session:
                print("! Warning: session number not matching: {} {}".format(session, self.session))

        self.date = xtree.xpath("//date//text()")[0]
        try:
            self.original_source = xtree.xpath("//sourceDesc//url//text()")[0]
        except IndexError:
            self.original_source = "NA"
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
        doc.text_source = "GermaParlTEI from " + self.original_source
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
                                                              'source_type': 'TEI/POI'}, verbosity=self.v)
                    if per is not None:
                        interjection.persons.add(per)
                    else:
                        print("! Warning: Speaker could not be identified")

    def run(self):

        self.get_or_create_objects()

        ### start parsing of speeches
        for div in self.divs:
            if self.v > 1:
                print("TEI div type: {}".format(div.get("type")))

            for sp in div.getchildren():
                if self.v > 1:
                    print("TEI current speaker: {}".format(sp.get("who")))
                # match speaker to database:
                info_dict = dict(sp.attrib)
                info_dict['wp'] = wp
                info_dict['session'] = self.session
                info_dict['source_type'] = 'TEI/SP'
                speaker = find_person_in_db(sp.get("who"), add_info=info_dict, verbosity=self.v)

                if speaker is None:
                    print(sp.get("who"))

                speaker_role_set = pm.SpeakerRole.objects.filter(alt_names__contains=[sp.get("role")])
                if len(speaker_role_set) < 1:
                    speaker_role = pm.SpeakerRole(name=sp.get("role"), alt_names=[sp.get("role")])
                    speaker_role.save()
                else:
                    speaker_role = speaker_role_set.first()
                    if len(speaker_role_set) > 1:
                        print("Warning: several speaker roles matching")

                text = []

                ut = pm.Utterance(
                    document=self.doc,
                    speaker=speaker,
                    speaker_role=speaker_role)
                ut.save()

                for c in sp.getchildren():
                    # tags: speaker (speaker), paragraph (p), interjection (stage)
                    if self.v > 1:
                        print("{}: {}".format(c.tag, c.text))
                    if c.tag == "p":
                        if c.text:
                            text.append(c.text.strip())
                    elif c.tag == "speaker":
                        if text:
                            para = self.create_paragraph(text, ut)
                            text = []
                    elif c.tag == "stage":
                        if text:
                            para = self.create_paragraph(text, ut)
                            text = []
                        self.add_interjections(c.text, para)
                    else:
                        print("unknown tag")
                if text:
                    para = self.create_paragraph(text, ut)






# =================================================================================================================

# main execution script
if __name__ == '__main__':

    sys.stdout = Logger()

    single_doc = False
    replace_docs = False
    tei_path = "/media/Data/MCC/Parliament Germany/GermaParlTEI-master"

    delete_all = False
    delete_additional_persons = False

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
        wp = 18
        session = 1

        xml_file = os.path.join(tei_path, "{wp:02d}/BT_{wp:02d}_{sn:03d}.xml".format(wp=wp, sn=session))
        namespaces = {'tei': 'http://www.tei-c.org/ns/1.0'}

        print("reading from {}".format(xml_file))

        xtree = etree.parse(xml_file)
        parser = parse_tei_items(xtree)

        # pm.Document.objects.filter(parlperiod__n=parser.wp, sitting=parser.session).delete()
        parser.run()
        print("Done.")

        exit()

    # go through all scripts iteratively
    for pperiod in range(13, 12, -1):
        for session in range(0, 300):

            xml_file = os.path.join(tei_path, "{wp:02d}/BT_{wp:02d}_{sn:03d}.xml".format(wp=pperiod, sn=session))

            if os.path.isfile(xml_file):
                print("reading from {}".format(xml_file))

                xtree = etree.parse(xml_file)
                if replace_docs:
                    pm.Document.objects.filter(parlperiod__n=pperiod, sitting=session).delete()
                pm.Document.objects.filter(parlperiod__n=pperiod, sitting=session,
                                           text_source__startswith="GermaParlTEI from ").delete()

                parser = parse_tei_items(xtree, period=pperiod, session=session)
                parser.run()

    print("Done")
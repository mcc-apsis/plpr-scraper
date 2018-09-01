
import re
from regular_expressions_global import *
from normdatei.parties import search_party_names

# import from appended path
import parliament.models as pm


def dehyphenate(text, nl=False):
    DEHYPHENATE = re.compile('(?<=[A-Za-zäöüß])(-\s*)\n(?!\s*[A-ZÄÖÜ][a-zäöüß])', re.M)

    if isinstance(text, (list, tuple)):
        text = '\n'.join(text)
    text = DEHYPHENATE.sub('', text)
    if nl:
        return text
    else:
        return text.replace('\n', ' ')

def dehyphenate_with_space(text):
    DEHYPHENATE_SPACE = re.compile('(?<=[A-Za-zäöüÄÖÜß])(-\s)(?![A-ZÄÖÜ][a-zäöüß])', re.M)

    if isinstance(text, (list, tuple)):
        text = '\n'.join(text)
    text = DEHYPHENATE_SPACE.sub('', text)
    return text.replace('\n', ' ')


def find_person_in_db(name, wp, add_info=dict(), create=True,
                      first_entry_for_unresolved_ambiguity=True, verbosity=1):
    name = INHYPHEN.sub(r'\1\2', name)
    name = name.replace('\n', ' ')
    name = NAME_REMOVE.sub('', name)

    position = PERSON_POSITION.search(name)
    if "position" in add_info.keys():
        if position:
            if add_info["position"] != position:
                print("! Warning: position does not match ({}, {})".format(
                    position, add_info["position"]))
        position = add_info["position"]

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
    if "party" in add_info.keys():
        add_info["party"] = add_info["party"].strip()
        if party:
            if add_info["party"] != party:
                print("! Warning: Parties not matching ({}, {})".format(party, add_info["party"]))
        party = add_info["party"]
    cname = PERSON_PARTY.sub(r'\1', cname)

    ortszusatz = PERSON_PARTY.match(cname)
    if ortszusatz:
        ortszusatz = ortszusatz.group(2)
    cname = PERSON_PARTY.sub(r'\1', cname)

    cname = correct_name_parsing_errors(cname)

    if len(cname.split(' ')) > 1:
        surname = cname.split(' ')[-1].strip('-– ()') # remove beginning and tailing "-", "(", ")" and white space
        firstname = cname.split(' ')[0].strip('-– ()')
    else:
        surname = cname.strip('-– ()')
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
        print("For name string: {}".format(name))
        print("first name: {}, surname: {}".format(firstname, surname))
        print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
        print("Entries: {}".format([pers.clean_name for pers in query]))

        if first_entry_for_unresolved_ambiguity:
            print('Taking first entry of ambiguous results')
            return query.first()
        elif create:
            person, created = pm.Person.objects.get_or_create(surname='Ambiguity', first_name='Ambiguity')
            return person
        else:
            return None

    else:
        if verbosity > 1:
            print("person not found by surname and wp, trying other methods.")
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
                    print("! Warning: party could not be identified: {}".format(party))
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
                person.clean_name = "{} {}".format(
                    person.first_name,
                    person.surname
                ).strip()
                if title:
                    person.title = title
                    person.clean_name = person.title + " " + person.clean_name
                if party:
                    try:
                        party_obj = pm.Party.objects.get(alt_names__contains=[party])
                        person.party = party_obj
                    except pm.Party.DoesNotExist:
                        print("! Warning: party could not be identified: {}".format(party))
                if ortszusatz:
                    person.ortszusatz = ortszusatz
                    person.clean_name += " " + ortszusatz

                # use position with data model "Post" ?

                person.in_parlperiod = [wp]
                person.information_source = "from protocol scraping: {}".format(name)
                person.save()
                print("Created person: {}".format(person))
                return person

            else:
                return None


# interjections (poi = point of interjection?)
class POI(object):
    def __init__(self, text):
        self.poitext = text
        self.speakers = []
        self.parties = ""
        self.type = None

        for m in re.findall(ABG, text):
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


# copied from normdatei.text
def clean_text(text):

    text = text.replace('\r', '\n')
    text = text.replace(u'\xa0', ' ')
    text = text.replace(u'\x96', '-')
    # text = text.replace(u'\u2014', '-')
    # text = text.replace(u'\u2013', '-')
    return text


def correct_name_parsing_errors(text):

    text = re.sub('(?<=[a-zäöüß])(-\s+)(?=[A-ZÄÖÜ][a-zäöüß])', '-', text.strip('-– ()').replace('--', '-'))

    return text


def correct_pdf_parsing_errors(text):

    # get rid of additional white space

    text = re.sub('\s+\.\s', '. ', text)
    text = re.sub('\s+\.\s*\n', '.\n', text)
    return text

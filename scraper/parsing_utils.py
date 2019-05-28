import re
from regular_expressions_global import *
from normality import normalize

# import from appended path
import parliament.models as pm
import cities.models as cmodels


# copied from normdatei.parties
def search_party_names(text):
    if text is None:
        return
    text = PARTIES_SPLIT.split(text)
    text = normalize(text[0])
    parties = set()
    for party, rex in PARTIES_REGEX.items():
        if rex.findall(text):
            parties.add(party)
    if not len(parties):
        return
    parties = ':'.join(sorted(parties))
    return parties


def dehyphenate(text, nl=False):
    """
    Removes hyphens from hyphenated words

    :param text: input text
    :param nl: boolean whether to return the text with newlines
    :return: dehyphenated text
    """
    DEHYPHENATE = re.compile('(?<=[A-Za-zäöüß])(-\s*)\n(?!\s*[A-ZÄÖÜ][a-zäöüß])', re.M)

    if isinstance(text, (list, tuple)):
        text = '\n'.join(text)
    text = DEHYPHENATE.sub('', text)
    if nl:
        return text
    else:
        return text.replace('\n', ' ')


def dehyphenate_with_space(text):
    """
    Joins hyphenated words that stem from joining lines ending with a hyphenation with a space (so far only used in TEI parser)

    :param text: input text
    :return: dehyphenated text
    """

    DEHYPHENATE_SPACE = re.compile('(?<=[A-Za-zäöüÄÖÜß])(-\s)(?![A-ZÄÖÜ][a-zäöüß])', re.M)

    if isinstance(text, (list, tuple)):
        text = '\n'.join(text)
    text = DEHYPHENATE_SPACE.sub('', text)
    return text.replace('\n', ' ')

# =================================================================================================

def emit_person(person, period=None, title="", party="", ortszusatz=""):
    # add information to person if it is not from the stammdata
    if person.information_source != "MDB Stammdata":
        if title:
            person.title = title
        if party:
            try:
                party_obj = pm.Party.objects.get(alt_names__contains=[party])
                person.party = party_obj
            except pm.Party.DoesNotExist:
                print("! Warning: party could not be identified in emit_person: {}".format(party))
        if ortszusatz:
            person.ortszusatz = ortszusatz
        if period:
            period_set = set(person.in_parlperiod)
            person.in_parlperiod = list(period_set.union({period}))

        person.save()

    return person

def find_person_in_db(name, add_info=dict(), create=True,
                      first_entry_for_unresolved_ambiguity=True, verbosity=1):

    if name.strip('-– ()') is '' or name is None:
        print("! Warning: no valid name string given")
        if create:
            person, created = pm.Person.objects.get_or_create(surname='Error: no valid string', first_name='')
            return person
        else:
            return None

    original_string = name

    if 'wp' in add_info.keys():
        wp = add_info['wp']
    else:
        wp = None

    name = clean_text(name)
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
        firstname = cname.split(' ')[0].strip('-– ()­')
    else:
        surname = cname.strip('-– ()')
        firstname = ''

    # find matching entry in database
    query = pm.Person.objects.filter(alt_surnames__contains=[surname], alt_first_names__contains=[firstname])

    if len(query) == 1:
        return query.first()

    elif len(query) > 1:
        if party:
            rquery = query.filter(party__alt_names__contains=[party])
            if len(rquery) == 1:
                return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
            elif len(rquery) > 1:
                query = rquery

        if ortszusatz:
            rquery = query.filter(ortszusatz=ortszusatz)
            if len(rquery) == 1:
                return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
            elif len(rquery) > 1:
                query = rquery

        if title:
            rquery = query.filter(title=title)
            if len(rquery) == 1:
                return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
            elif len(rquery) > 1:
                query = rquery

        if wp:
            rquery = query.filter(in_parlperiod__contains=[wp])
            if len(rquery) == 1:
                return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
            elif len(rquery) > 1:
                query = rquery

        print("! Warning: Could not distinguish between persons!")
        print("For name string: {}".format(name))
        print("first name: {}, surname: {}".format(firstname, surname))
        print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
        print("Query: {}".format(query))
        print("Clean names: {}".format([pers.clean_name for pers in query]))

        if first_entry_for_unresolved_ambiguity:
            print('Taking first entry of ambiguous results')
            return query.first()
        else:
            return None

    # if query returns no results
    else:
        print("Person not found in database: {}".format(cname))
        if verbosity > 0:
            print("name: {}".format(name))
            print("first name: {}, surname: {}".format(firstname, surname))
            print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))

        if create:
            person = pm.Person(surname=surname, first_name=firstname)
            if title:
                person.title = title
            if party:
                try:
                    party_obj = pm.Party.objects.get(alt_names__contains=[party])
                    person.party = party_obj
                except pm.Party.DoesNotExist:
                    print("! Warning: party could not be identified when creating new person in find_person_in_db: {}".format(party))
            if ortszusatz:
                person.ortszusatz = ortszusatz

            if position:
                person.positions = [position]
                # use position with data model "Post" ?

            if 'session' in add_info.keys():
                session_str = "{sn:03d}".format(sn=add_info['session'])
            else:
                session_str = "???"

            if 'source_type' in add_info.keys():
                source_str = add_info['source_type']
            else:
                source_str = ""

            person.in_parlperiod = [wp]
            person.active_country = cmodels.Country.objects.get(name='Germany')
            person.information_source = "from protocol scraping " \
                                        "{wp:02d}/{sn} {type}: {name}".format(wp=wp, sn=session_str,
                                                                              type=source_str, name=original_string)
            person.save()
            print("Created person: {}".format(person))
            return person

        else:
            return None


# interjections (poi = point of interjection?)
class POI(object):
    def __init__(self, text):
        self.poitext = clean_text(text)
        self.speakers = []
        self.speaker_ortszusatz = ""
        self.speaker_party = ""
        self.parties = ""
        self.type = None

        for m in re.findall(ABG, text):
            self.speakers.append(m[0].strip())
            text = text.replace(m[1],"")
        if ": " in text:
            sinfo = text.split(': ', 1)
            speaker = sinfo[0].split(' ')
            speaker_match = PARTY_MEMBER_PDF_POI.match(sinfo[0])

            if speaker_match:
                self.speakers.append(speaker_match.group(1))
                self.speaker_party = search_person_party(speaker_match.group(3))

                if speaker_match.group(2) is not None:
                    self.speaker_ortszusatz = REMOVE_BRACKET.match(speaker_match.group(2)).group(1)

            #else:
            #    self.parties = search_party_names(speaker[0].strip())

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
    text = text.replace(u'\xad', '-')
    text = text.replace(u'\u2014', '–')
    # text = text.replace(u'\u2013', '–')
    return text


def correct_name_parsing_errors(text):

    text = re.sub('(?<=[a-zäöüß])(-\s+)(?=[A-ZÄÖÜ][a-zäöüß])', '-', text.strip('-– ()').replace('--', '-'))

    return text


def correct_pdf_parsing_errors(text):

    # get rid of additional white space

    text = re.sub('\s+\.\s', '. ', text)
    text = re.sub('\s+\.\s*\n', '.\n', text)
    return text


# adapted from normdatei.text
def fingerprint(name):
    if name is None:
        return
    name = FP_REMOVE.sub(' ', name.strip())
    return normalize(name).replace(' ', '-')


def search_person_party(text):
    """
    returns the party of a speaker from an input string (from raw text)
    """
    if text is None:
        return
    text = normalize(text)
    # identify correct group of text in name

    # find party
    for party, rex in PARTIES_REGEX_PDF.items():
        if rex.findall(text):
            person_party = party
            return person_party

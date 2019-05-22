import re
from regular_expressions_global import *
from normality import normalize

import sys
sys.path.append('/home/leey/Documents/Data/tmv/BasicBrowser/')
import parliament.models as pm
import cities.models as cmodels
from django.db.models import Q

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
                print("! Warning: party could not be identified: {}".format(party))
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
    name = name.rstrip(':') # new
    name = name.replace('\n', ' ')
    name = NAME_REMOVE.sub('', name)

    name_match = re.search('\(', name)
    if name_match:
        name = name.split('(')[1]

    position = PERSON_POSITION.search(name)
    if "position" in add_info.keys():
        if position:
            if add_info["position"] != position:
                print("! Warning: position does not match ({}, {})".format(
                    position, add_info["position"]))
        position = add_info["position"]

    if position and verbosity > 1:
        print("= position: {}".format(position.group(0)))

    if position:
        position = position.group(0)
    cname = PERSON_POSITION.sub('', name).strip(' ,')

    title = TITLE.match(cname)
    if title:
        title = title.group(0)
    cname = TITLE.sub('', cname).strip()

    frau = FRAU.match(cname)
    if frau:
        frau = frau.group(0)
        # gender = 1
    cname = FRAU.sub('', cname).strip()

    if "party" in add_info.keys():
        party = add_info["party"]
    cname = PERSON_PARTY.sub(r'\1', cname)

    if "ortszusatz" in add_info.keys():
        ortszusatz = add_info["ortszusatz"]

    cname = correct_name_parsing_errors(cname)

    if len(cname.split(' ')) > 1:
        surname = cname.split(' ')[-1].strip('-– ()') # remove beginning and tailing "-", "(", ")" and white space
        firstname = cname.split(' ')[0].strip('-– ()­')
    else:
        surname = cname.strip('-– ()')
        firstname = ''

    # accounting for 'von' surnames
    if firstname == 'von':
    #    surname = firstname + ' ' + surname
        firstname = ''

    # find matching entry in database
    query = pm.Person.objects.filter(alt_surnames__contains=[surname])

    if len(query) == 1:
        return query.first()

    elif len(query) > 1:
        rquery = query.filter(information_source="MDB Stammdata")
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

        if firstname:  # empty string '' gives False
            rquery = query.filter(alt_first_names__contains=[firstname])
            if len(rquery) == 1:
                return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
            elif len(rquery) > 1:
                query = rquery

        if party:
            rquery = query.filter(Q(party__name=party) | Q(party__alt_names__contains=[party]))
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

        if position:
            rquery = query.filter(positions__contains=[position])
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

        # if gender:
        #    rquery = query.filter(gender=gender)
        #    if len(rquery) == 1:
        #        return emit_person(rquery.first(), period=wp, title=title, party=party, ortszusatz=ortszusatz)
        #    elif len(rquery) > 1:
        #        query = rquery

        print("! Warning: Could not distinguish between persons!")
        print("For name string: {}".format(name))
        print("first name: {}, surname: {}".format(firstname, surname))
        print("title: {}, party: {}, position: {}, ortszusatz: {}".format(title, party, position, ortszusatz))
        print("Query: {}".format(query))
        #print("Clean names: {}".format([pers.clean_name for pers in query]))

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
                    print("! Warning: party could not be identified: {}".format(party))
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

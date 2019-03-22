# coding: utf-8
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
from bs4 import BeautifulSoup

import pprint

sys.path.append('/home/galm/software/django/tmv/BasicBrowser/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
django.setup()

from parliament.models import *
from cities.models import *

CURRENT_PARL = 19
WK_EXP = re.compile('Wahlkreis ([0-9]*): (.*).*')
WKE_EXP = re.compile('Wahlkreisergebnis: ([0-9,]*) *%(.*)')

PARTY_TRANSLATION = {
    'DIE GRÜNEN': 'gruene',
    'DIE LINKE': 'linke',
    'CSU': 'cducsu',
    'CDU': 'cducsu'
}


def cycle_parliaments():
    print("fetching mps")
    url = 'https://www.abgeordnetenwatch.de'
    res = requests.get(url)
    soup =  BeautifulSoup(res.content,'html.parser')
    parls = soup.find_all('a', {'class': 'header__subnav__archive__list__item__link'})

    parl, created = Parl.objects.get_or_create(
        country=Country.objects.get(name="Germany"),
        level='N'
    )

    # Go through each parliament
    for i,p in enumerate(parls):

        ps, created = ParlSession.objects.get_or_create(
            parliament=parl,
            n=CURRENT_PARL-i
        )

        print('Parliament: ' + p.text)
        purl = url+p['href']+'/profile'
        res = requests.get(purl)
        soup =  BeautifulSoup(res.content,'html.parser')
        last_page = soup.find('li', {'class':'pager__item--last'})
        lp = int(last_page.find('a')['href'].split('page=')[1])
        print(lp)
        for pn in range(lp+1):
            page_url = purl+'?page='+str(pn)
            res = requests.get(page_url)
            soup =  BeautifulSoup(res.content,'html.parser')
            tiles = soup.find_all('div',{'class': 'deputy tile'})
            for t in tiles:
                dep_url = url+t.find('a')['href']
                res = requests.get(dep_url)
                soup = BeautifulSoup(res.content, 'html.parser')
                name = soup.find('h1', {'class': 'deputy__title'}).text
                print(name)
                sc = clean_name(name)
                fp = fingerprint(sc)
                party = soup.find('div', {'class':'party-indicator'}).find('a').text

                print(party)
                try:
                    party = Party.objects.get(
                        name__iexact=party
                    )
                except:
                    party = Party.objects.get(
                        name__iexact=PARTY_TRANSLATION[party]
                    )
                print(party)
                dts = soup.find_all('dt')
                dds = soup.find_all('dd')

                #person, created = Person.objects.get_or_create()
                per, created = Person.objects.get_or_create(
                    surname=fp.split('-')[-1],
                    first_name=fp.split('-')[0],
                )
                per.clean_name = sc
                #print(sc)
                per.save()

                if per.party is None:
                    per.party = party

                per.save()
                for d in range(len(dts)):
                    if dts[d].text.strip() == "Wahlkreis":
                        wk = dds[d].find_all('p')[0].text.strip()
                        wkmatch = WK_EXP.match(wk)
                        constituency, created = Constituency.objects.get_or_create(
                            parliament=parl,
                            number=wkmatch.group(1),
                            name=wkmatch.group(2).strip()
                        )

                        wke = dds[d].find_all('p')[1].text.strip()
                        print(wke)
                        wkematch = WKE_EXP.match(wke)
                        cr1, created = ConstituencyVote1.objects.get_or_create(
                            parlsession=ps,
                            person=per,
                            constituency=constituency,
                            proportion=float(wkematch.group(1).replace(',','.'))
                        )
                        seat, created = Seat.objects.get_or_create(
                            parlsession=ps,
                            occupant=per
                        )
                        seat.party=party
                        if "Liste" in wke:
                            seat.seat_type=Seat.LIST
                        else:
                            seat.seat_type=Seat.DIRECT
                            seat.constituency=constituency
                        seat.save()
                    if dts[d].text.strip() == "Liste":
                        l = dds[d].text.strip()
                        lname=l.split(',')[0]
                        plist, created =PartyList.objects.get_or_create(
                            name = lname,
                            parlsession=ps,
                        )
                        try:
                            region = cities.models.Region.objects.get(
                                name=lname.replace('Landesliste','').strip()
                            )
                            plist.region=region
                            plist.save()
                        except:
                            pass
                        if "Liste" in wke:
                            seat.list=plist
                            seat.save()





    return


if __name__ == '__main__':
    Constituency.objects.all().delete()
    PartyList.objects.all().delete()
    Seat.objects.all().delete()
    cycle_parliaments()

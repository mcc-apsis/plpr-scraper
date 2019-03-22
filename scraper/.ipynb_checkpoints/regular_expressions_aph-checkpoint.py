import re

# regex notes:
# . matches anything except newline (if DOTALL flag is raised, it also matches newline)
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
    'January':1,
    'February':2,
    'March':3,
    'April': 4,
    'May': 5,
    'June': 6,
    'July': 7,
    'August': 8,
    'September': 9,
    'October': 10,
    'November': 11,
    'December': 12
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

END_MARK = re.compile('(\(Schluss:.\d{1,2}\s?.\d{1,2}.Uhr\).*|\(*Schluss der Sitzung)|Die Sitzung ist geschlossen')

HEADER_MARK = re.compile('\d{0,6}\s*Deutscher Bundestag\s*[–\-]\s*\d{1,2}\.\s*Wahlperiode\s*[–\-]\s*\d{1,4}\. Sitzung.\s*(Bonn|Berlin),'
                         '|\s*\([A-Z]\)(\s*\([A-Z]\))*\s*$|\d{1,6}\s*$|^\s*\([A-Z]\)\s\)$|^\s*\([A-Z]$|^\s*[A-Z]\)$')

PARTIES_REGEX = {
    'labor': re.compile('LP'),
    'liberal': re.compile('ALP'),
    'national': re.compile('NP|NATS'),
    'independent': re.compile('IND'),
}

ANY_PARTY = re.compile('({})'.format('|'.join([x.pattern.strip() for x in PARTIES_REGEX.values()])))

# speaker type matches
# longest mdb name has 44 chars
PARTY_MEMBER = re.compile('\s*(.{4,50}?\(([^\(\)]*)\)):\s*')
PRESIDENT = re.compile('\s*((Alterspräsident(?:in)?|Vizepräsident(?:in)?|Präsident(?:in)?)\s.{5,50}?):\s*')
STAATSSEKR = re.compile('\s*([^\n]{4,50}?, (Par[li]\s?\.\s)?Staatssekretär.*?):\s*', re.DOTALL)
STAATSMINISTER = re.compile('\s*([^\n]{4,50}?, Staatsminister.*?):\s*', re.DOTALL)
MINISTER = re.compile('\s*([^\n]{4,50}?, Bundesminister.*?):\s*', re.DOTALL)
WEHRBEAUFTRAGTER = re.compile('\s*(.{4,50}?, Wehrbeauftragter.*?):\s*')
BUNDESKANZLER = re.compile('\s*(.{4,50}?, Bundeskanzler(in)?.*?):\s*')
BEAUFTRAGT = re.compile('\s*(.{4,50}?, Beauftragter? der Bundes.*):\s*')
BERICHTERSTATTER = re.compile('\s*(.{4,50}?, Berichterstatter(in)?.*?):\s*')

PERSON_POSITION = ['Speaker', 'Deputy Speaker']

PERSON_POSITION = re.compile(u'(%s)' % '|'.join(PERSON_POSITION), re.U)

NAME_REMOVE = [u'\\[.*\\]|\\(.*\\)', u' de[sr]', u'Gegenrufe?', 'Weiterer Zuruf', 'Zuruf', 'Weiterer',
               u', zur.*', u', auf die', u' an die', u', an .*', u'gewandt', 'Liedvortrag', '#',
               'Beifall', ' bei der', u'\\d{1,20}', 'Widerspruch', 'Lachen', 'Heiterkeit']
NAME_REMOVE = re.compile(u'(%s)' % '|'.join(NAME_REMOVE), re.U)

PERSON_PARTY = re.compile('\s*(.{4,140})\s\((.*)\)$')
TITLE = re.compile('[A-Z]?\.?\s*Dr\s?.|Mr\.|Ms\.|Mrs\.|Madam')

TOP_MARK = re.compile('.*(?: rufe.*der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt)(.*)')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
POI_BEGIN = re.compile('\(\s*[A-Z][^)]+$')
POI_END = re.compile('^[^(]+\)')

WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

ABG = 'Abg\.\s*(.{4,50}?)(\[[\wäöüßÄÖÜ /]*\])'
INHYPHEN = re.compile(r'([a-zäöüß])-\s?([a-zäöüß])', re.U)

FP_REMOVE = re.compile(u'(^.*Dr.?( h.? ?c.?)?| (von( der)?)| [A-Z]\. )')

# -!- coding:utf-8 -!-
import os
import unittest

from scraper.scraper_pdfscans import SpeechParser


class TestSpeechParser(unittest.TestCase):
    def test_basic(self):
        text = """
Beginn: 12:30 Uhr
  
  Präsident Dr. Norbert Lammert: 
  Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
  Ich eröffne die Aussprache und erteile das Wort zunächst dem Bundesminister Hermann Gröhe.
(Beifall bei der CDU/CSU und der SPD)
  Hermann Gröhe, Bundesminister für Gesundheit: 
  Herr Präsident! Liebe Kolleginnen! Liebe Kollegen!
(Beifall bei der CDU/CSU und der SPD)
  Wir tun was!
"""
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.\n\nIch eröffne die Aussprache und erteile das Wort zunächst dem Bundesminister Hermann Gröhe.'}
        second = {'speaker': None,
                  'type': 'poi',
                  'text': 'Beifall bei der CDU/CSU und der SPD'}
        third = {'speaker': 'Hermann Gröhe, Bundesminister für Gesundheit',
                 'type': 'speech',
                 'text': 'Herr Präsident! Liebe Kolleginnen! Liebe Kollegen!'}
        fourth = {'speaker': None,
                  'type': 'poi',
                  'text': 'Beifall bei der CDU/CSU und der SPD'}
        fifth = {'speaker': 'Hermann Gröhe, Bundesminister für Gesundheit',
                 'type': 'speech',
                 'text': 'Wir tun was!'}

        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)
        self.assertEqual(lines[2], third)
        self.assertEqual(lines[3], fourth)
        self.assertEqual(lines[4], fifth)

    def test_tops(self):
        text = """
Beginn: 12:30 Uhr

  Präsident Dr. Norbert Lammert: 
  Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
  Ich rufe Tagesordnungspunkt 1 auf.
Hermann Gröhe, Bundesminister für Gesundheit:
  Dankesehr! Jetzt rede ich! 
Jakob Mierscheid (CDU): 
  Wichtig ist auch Tagesordnungspunkt 4!
        """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.\n\nIch rufe Tagesordnungspunkt 1 auf.'}
        second = {'speaker': 'Hermann Gröhe, Bundesminister für Gesundheit',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        third = {'speaker': 'Jakob Mierscheid (CDU)',
                 'type': 'speech',
                 'text': 'Wichtig ist auch Tagesordnungspunkt 4!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)
        self.assertEqual(lines[2], third)

    def test_staatssekr(self):
        text = """
Beginn: 12:30 Uhr

  Präsident Dr. Norbert Lammert: 
  Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
Dr. Michael Meister, Parl. Staatssekretär beim Bundesminister der Finanzen:
  Dankesehr! Jetzt rede ich! 
        """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        second = {'speaker': 'Dr. Michael Meister, Parl. Staatssekretär beim Bundesminister der Finanzen',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)

    def test_staatsminister(self):
        text = """
Beginn: 12:30 Uhr

  Präsident Dr. Norbert Lammert: 
  Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
Dr. Maria Böhmer, Staatsministerin bei der Bundeskanzlerin:
  Dankesehr! Jetzt rede ich! 
            """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        second = {'speaker': 'Dr. Maria Böhmer, Staatsministerin bei der Bundeskanzlerin',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)


    def test_multiple_tops(self):
        dir = os.path.dirname(__file__)
        filename = os.path.join(dir, './extract.txt')
        with open(filename) as infile:
            data = infile.readlines()
        parser = SpeechParser(data)
        lines = list(parser)
        self.assertEqual(lines[0]['speaker'], 'Vizepräsidentin Petra Pau')
        self.assertEqual(lines[1]['type'], 'poi')
        self.assertEqual(lines[2]['speaker'], 'Vizepräsidentin Petra Pau')
        self.assertEqual(lines[3]['speaker'], 'Harald Ebner (BÜNDNIS 90/DIE GRÜNEN)')
        self.assertEqual(lines[4]['speaker'], 'Vizepräsidentin Petra Pau')
        self.assertEqual(lines[5]['type'], 'poi')
        self.assertEqual(lines[6]['speaker'], 'Vizepräsidentin Petra Pau')

    def test_begin_without_colon(self):
        text = """
Beginn 9.00 Uhr

  Präsident Dr. Norbert Lammert: 
  Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
            """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        self.assertEqual(lines[0], first)

    def test_wehrbeauftragter(self):
        text = """
Beginn: 12:30 Uhr

Präsident Dr. Norbert Lammert: 
Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
Dr. Hans-Peter Bartels, Wehrbeauftragter des Deutschen Bundestages:
Dankesehr! Jetzt rede ich! 
            """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        second = {'speaker': 'Dr. Hans-Peter Bartels, Wehrbeauftragter des Deutschen Bundestages',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)

    def test_bundeskanzlerin(self):
        text = """
Beginn: 12:30 Uhr

Präsident Dr. Norbert Lammert: 
Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
Dr. Angela Merkel, Bundeskanzlerin:
Dankesehr! Jetzt rede ich! 
            """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        second = {'speaker': 'Dr. Angela Merkel, Bundeskanzlerin',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)

    def test_bundesbeauftragte(self):
        text = """
Beginn: 12:30 Uhr

Präsident Dr. Norbert Lammert: 
Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.
Iris Gleicke, Beauftragte der Bundesregierung für die neuen Bundesländer::
Dankesehr! Jetzt rede ich! 
            """
        parser = SpeechParser(text.split('\n'))
        lines = list(parser)
        first = {'speaker': 'Präsident Dr. Norbert Lammert',
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.'}
        second = {'speaker': 'Iris Gleicke, Beauftragte der Bundesregierung für die neuen Bundesländer:',
                  'type': 'speech',
                  'text': 'Dankesehr! Jetzt rede ich!'}
        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)


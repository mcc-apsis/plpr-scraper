# -!- coding:utf-8 -!-
import unittest

from scraper.scraper import SpeechParser


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
                 'in_writing': False,
                 'type': 'chair',
                 'text': 'Nehmen Sie bitte Platz. Die Sitzung ist eröffnet.\n\nIch eröffne die Aussprache und erteile das Wort zunächst dem Bundesminister Hermann Gröhe.'}
        second = {'speaker': None,
                  'in_writing': False,
                  'type': 'poi',
                  'text': 'Beifall bei der CDU/CSU und der SPD'}
        third = {'speaker': 'Hermann Gröhe, Bundesminister für Gesundheit',
                 'in_writing': False,
                 'type': 'speech',
                 'text': 'Herr Präsident! Liebe Kolleginnen! Liebe Kollegen!' }
        fourth = {'speaker': None,
                  'in_writing': False,
                  'type': 'poi',
                  'text': 'Beifall bei der CDU/CSU und der SPD'}
        fifth = {'speaker': 'Hermann Gröhe, Bundesminister für Gesundheit',
                 'in_writing': False,
                 'type': 'speech',
                 'text': 'Wir tun was!'}

        self.assertEqual(lines[0], first)
        self.assertEqual(lines[1], second)
        self.assertEqual(lines[2], third)
        self.assertEqual(lines[3], fourth)
        self.assertEqual(lines[4], fifth)

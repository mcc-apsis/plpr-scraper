#!/usr/bin/env python

from distutils.core import setup

setup(name='plpr-scraper',
      version='1.0',
      description='scraper',
      author='Greg Ward',
      author_email='gward@python.net',
      url='https://www.python.org/sigs/distutils-sig/',
      packages=['scraper'],
      scripts=['bin/bundesscrape'],
     )
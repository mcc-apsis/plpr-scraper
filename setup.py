"""This is setup.py of abacra"""

from setuptools import setup

# for developers: recommended way of installing is to run in this directory
# pip install -e .
# This creates a link insteaed of copying the files,
# so modifications in this directory are
# modifications in the installed package.

setup(name="parliamentscraper",
      version="",
      description="",
      url="",
      author="",
      license="",
      packages=["scraper"],
      install_requires=[
            "numpy>=1.11.0"      ],
      # see http://stackoverflow.com/questions/15869473/
      # what-is-the-advantage-of-setting-zip-safe-to-true-when-packaging-a-python-projec
      zip_safe=False
      )

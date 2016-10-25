# -*- coding: utf-8 -*-

from setuptools import setup
import re

# https://stackoverflow.com/questions/458550/standard-way-to-embed-version-into-python-package#7071358
VERSIONFILE = "pdblp/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." %
                       (VERSIONFILE,))

#http://stackoverflow.com/questions/10718767/have-the-same-readme-both-in-markdown-and-restructuredtext#23265673
try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print("warning: pypandoc module not found, could not convert Markdown to RST")
    read_md = lambda f: open(f, 'r').read()

setup(name='pdblp',
      version=verstr,
      description='Bloomberg Open API with pandas',
      long_description=read_md('README.md'),
      url='https://github.com/MatthewGilbert/pdblp',
      author='Matthew Gilbert',
      author_email='matthew.gilbert12@gmail.com',
      license='MIT',
      platforms='any',
      install_requires=['pandas>=0.18.0'],
      packages=['pdblp', 'pdblp.tests'],
      test_suite='pdblp.tests',
      zip_safe=False)

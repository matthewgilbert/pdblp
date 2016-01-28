# -*- coding: utf-8 -*-

from setuptools import setup
import re

VERSIONFILE="pdblp/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." %
                       (VERSIONFILE,))

setup(name='pdblp',
      version=verstr,
      description='Bloomberg Open API with pandas',
      url='https://github.com/MatthewGilbert/pdblp',
      author='MatthewGilbert',
      author_email='matthew.gilbert12@gmail.com',
      license='MIT',
      packages=['pdblp'],
      zip_safe=False)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# To create a distribution package for pip or easy-install:
# python setup.py sdist
from setuptools import setup, find_packages, Command
from os.path import join, dirname, realpath
from warnings import warn

author = u"Richard Hartmann"
authors = [author, u"Paul Müller"]
name = 'jobmanager'
description = 'Python job manager for parallel computing.'
year = "2015"

DIR = realpath(dirname(__file__))

longdescription = open(join(DIR, "doc/description.txt"), "r").read().strip()

class PyTest(Command):
    user_options = []
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys,subprocess
        errno = subprocess.call([sys.executable, 'tests/runtests.py'])
        raise SystemExit(errno)

try:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/"+name)
    # get version number
    from jm_version import __version__ as version
except:
    version = "unknown"



setup(
    name=name,
    author=author,
    url='https://github.com/cimatosa/jobmanager',
    version=version,
    packages=[name],
    package_dir={name: name},
    license="MIT",
    description=description,
    long_description=longdescription,
    install_requires=["sqlitedict>=1.2.0", "NumPy>=1.5.1"],
    tests_require=["psutil"],
    keywords=["multiprocessing", "queue", "parallel", "distributed", "computing",
              "progress", "manager", "job", "persistent data", "scheduler"],
    classifiers= [
        'Operating System :: OS Independent',
        #'Programming Language :: Python :: 2.7', #Todo
        #'Programming Language :: Python :: 3.2', # also not very well tested
        #'Programming Language :: Python :: 3.3', # also not very well tested
        'Programming Language :: Python :: 3.4',
        'Intended Audience :: Science/Research'
                 ],
    platforms=['ALL'],
    cmdclass = {'test': PyTest},
    )



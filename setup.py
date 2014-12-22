#!/usr/bin/env python
# To create a distribution package for pip or easy-install:
# python setup.py sdist
from setuptools import setup, find_packages
from os.path import join, dirname, realpath
from warnings import warn

name='jobmanager'





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
    author='Richard Hartmann',
    #author_email='richard.hartmann...',
    url='https://github.com/cimatosa/jobmanager',
    version=version,
    packages=[name],
    package_dir={name: name},
    license="MIT",
    description='Python job manager for parallel computing.',
    long_description="""easy distributed computing based on the python
class SyncManager for remote communication
and python module multiprocessing for local
parallelism.""",
    install_requires=["sqlitedict", "NumPy>=1.5.1"],
    # tests: psutil
    keywords=["multiprocessing", "queue", "parallel",
              "progress", "manager", "job"],
    classifiers= [
        'Operating System :: OS Independent',
        #'Programming Language :: Python :: 2.7', #Todo
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Intended Audience :: Science/Research'
                 ],
    platforms=['ALL']
    )



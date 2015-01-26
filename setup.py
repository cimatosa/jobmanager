#!/usr/bin/env python
# To create a distribution package for pip or easy-install:
# python setup.py sdist
from setuptools import setup, find_packages, Command
from os.path import join, dirname, realpath
from warnings import warn

name='jobmanager'

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
    keywords=["multiprocessing", "queue", "parallel", "distributed", "computing",
              "progress", "manager", "job", "persistent data"],
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



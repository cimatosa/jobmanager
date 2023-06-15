jobmanager
==========
[![PyPI](http://img.shields.io/pypi/v/jobmanager.svg)](https://pypi.python.org/pypi/jobmanager)

Easy distributed computing based on the python class SyncManager for remote communication and python module multiprocessing for local parallelism.

### Documentation (out of date)
The documentation is available at http://cimatosa.github.io/jobmanager/ 

### Developer's note (out of date)
After cloning into jobmanager, create a virtual environment

    virtualenv --system-site-packages ve_jm
    source ve_jm/bin/activate

Install all dependencies

    python setup.py develop
    
Running an example

    python examples/simple_example.py

Running tests

    python setup.py test

jobmanager
==========
[![PyPI](http://img.shields.io/pypi/v/jobmanager.svg)](https://pypi.python.org/pypi/jobmanager)
[![Travis](http://img.shields.io/travis/cimatosa/jobmanager.svg?label=tests)](https://travis-ci.org/cimatosa/jobmanager)
[![codecov](https://codecov.io/gh/cimatosa/jobmanager/branch/master/graph/badge.svg)](https://codecov.io/gh/cimatosa/jobmanager)


Easy distributed computing based on the python class SyncManager for remote communication and python module multiprocessing for local parallelism.

### Documentation
The documentation is available at http://cimatosa.github.io/jobmanager/ 

### TODO
  * timeout for client
  * single proxy for client and queue for subprocesses
  * user signal to server to retrieve status


### Developer's note
After cloning into jobmanager, create a virtual environment

    virtualenv --system-site-packages ve_jm
    source ve_jm/bin/activate

Install all dependencies

    python setup.py develop
    
Running an example

    python examples/simple_example.py

Running tests

    python setup.py test
    
[travis test]

### Test Scripts


This will run all tests:

    python runtests.py

Beautiful html output is possible with (Unix, package `aha` required)

    ./runtests_html.sh


### Running single tests

Directly execute the scripts, e.g.


#### Client


    python test_clients.py


#### Progress


    python test_progress.py



#### Jobmanager Server-Client


Server:

    python test_jobmanager.py server
    

Client

    python test_jobmanager.py

#!/bin/bash

OUTFILE='pytest.html'

echo "run py.test ..."
(date; py.test --color=yes) | aha --black --title "pytest output for jobmanager module"> $OUTFILE
echo "done! (output written to $OUTFILE)"

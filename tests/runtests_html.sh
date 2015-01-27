#!/bin/bash
# Go to script directory
cd $(dirname $0)

OUTFILE='pytest.html'

echo "Working directory: $(pwd)"
echo "Running py.test ..."
(date; python runtests.py --color=yes) | aha --black --title "pytest output for jobmanager module"> $OUTFILE
echo "Done! (output written to $OUTFILE)"

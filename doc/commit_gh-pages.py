#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess as sp

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# commit changes of master
print("Automatically commiting/pushing changes to master")
sp.check_output(["git", 'commit', '-a', '-m', '"automated commit before doc upload"'])
sp.check_output(["git", 'push'])

# checkout the gh-pages branch
sp.check_output(["git", 'checkout', 'gh-pages'])

# copy built files
if os.system("cp -r ./build/html/* ../") != 0:
    sys.exit()

for item in os.listdir("./build/html/"):
    # Make sure we have added all files from html
    os.system("git add {}".format(item)

# commit changes
if len(sp.check_output(["git", "diff"]).strip()) > 0:
    sp.check_output(["git", 'commit', '-a', '-m', '"automated doc upload"'])

# push
try:
    sp.check_output(["git", 'push'])
except:
    print("Could not push to gh-pages.")

# go back to master
sp.check_output(["git", 'checkout', 'master'])

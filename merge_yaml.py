import time, glob
import os
import sys

outfilename = sys.argv[1] + ".yml"

filenames = glob.glob('temp/*.yml')

with open(outfilename, 'w') as outfile:
    for fname in filenames:
        with open(fname, 'r') as readfile:
            infile = readfile.read()
            for line in infile:
                outfile.write(line)
            #outfile.write("")
os.system('python all_removals.py'+' '+'"'+sys.argv[1]+'"')

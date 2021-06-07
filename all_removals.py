import sys
import os
import shutil
import time
from pathlib import Path
# 'r+' allows you to read and write to a file
with open(sys.argv[1]+".yml", "r+") as f:
    lines = f.readlines()
    f.seek(0)
    for line in lines:
        if not line.startswith(' target'):
            f.write(line)
    f.truncate()
# 'r+' allows you to read and write to a file
with open(sys.argv[1]+".yml", "r+") as f:
    lines = f.readlines()
    f.seek(0)
    for line in lines:
        if not line.startswith("  't"):
            f.write(line)
    f.truncate()
# Read in the file
with open(sys.argv[1]+'.yml', 'r') as file :
  filedata = file.read()
filedata = filedata.replace("',\n }", "'\n }")
with open(sys.argv[1]+'.yml', 'w') as file:
  file.write(filedata)
# Read in the file
with open(sys.argv[1]+'.yml', 'r') as file :
  filedata = file.read()
filedata = filedata.replace("schema.txt',", "schema.txt'")
with open(sys.argv[1]+'.yml', 'w') as file:
  file.write(filedata)
print("[INFO]: Completed Raw To Curartion Pipeline")
with open(sys.argv[1]+'.yml', 'a') as runscrpt:
    runscrpt.write("...")
#os.remove(r'temp/*.csv')
dirpath="temp"
shutil.rmtree(dirpath)
os.mkdir(dirpath)
dirpath2="input/out/"
shutil.rmtree(dirpath2)
os.mkdir(dirpath2)
filename="input/out/raw_to_curation.ok"
open(filename, 'a')
print("[INFO]: Completed Raw to Raw Pipeline")
#time.sleep(5)
#res = ''.join(random.choices(string.ascii_uppercase +string.digits, k = N))
#N = 7
Path(sys.argv[1]+'.yml').rename("input/YML/"+sys.argv[1]+"_"+str((int(time.time())))+'.yml')
#os.system('python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"')

import csv
import os
import sys
cmd = 'python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"'
#os.system('python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"')
returned_value = os.system(cmd)
print(returned_value)
if not returned_value:
    cmd = 'python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"'
    os.system(cmd)
else:
    cmd = 'python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"'
    os.system(cmd)
#python .\Run_YML_Creater.py "TIMS_Mapping_Sheet" "TIMS_Mapping_Sheet" "TIM_Config"
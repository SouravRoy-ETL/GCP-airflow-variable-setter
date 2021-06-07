# python .\Automated_VF_HU_RR_RC_YML_Creation.py "Netcracker_D1S4_Mapping_sheet_With PII_v7" "NC D1-S4 MAPPING SHEET" "D1S4_NC"
import pandas as pd
import sys
import os
import os.path
from os import path
read_file = pd.read_excel ("input/"+sys.argv[1]+".xlsx",sheet_name=sys.argv[2])
read_file.to_csv ("input/"+sys.argv[1]+".csv",
				index = None,
				header=True)
csv_df1 = pd.read_csv(r"input/"+sys.argv[1]+".csv",encoding='latin-1', sep=',', header=None, skiprows=0, na_values=' ', error_bad_lines=False)
if not os.path.exists("input/out/raw_to_curation.ok"):
	os.system('python RC_etl_before_yaml_conv.py'+" "+ '"'+sys.argv[1]+ '"'+' '+sys.argv[3])
else:
	os.system('python RR_etl_before_yaml_conv.py'+" "+ '"'+sys.argv[1]+ '"'+' '+sys.argv[3])

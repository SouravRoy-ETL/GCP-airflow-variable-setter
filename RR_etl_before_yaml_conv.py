################################################################################################
########### RAW TO RAW FRAMEWORK
################################################################################################
#convert ETL Code from Talend To Python
#Usage -
# on command line - python .\RR_etl_before_yaml_conv.py "Netcracker_D1S4_Mapping_sheet_With PII_v7" "D1S4_NC"
import pandas as pd
import numpy as np
import sys
import os
#Step 1 - Group By and Aggregate in form of list
row1 = pd.read_csv('input/'+sys.argv[1]+'.csv',encoding='utf8')
tAggregaterow = row1.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_NAME':lambda x: list(x)}).reset_index()
tAggregaterow.to_csv('input/'+sys.argv[1]+'_temp1.csv', index=False,encoding='utf8')
#Step 2 - TMAP - Remove the specific single quotes and replace "," with ",:" from TARGET_COLUMN_NAME
row2 = pd.read_csv('input/'+sys.argv[1]+'_temp1.csv')
row2.replace('', np.nan, inplace=True)
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(r"[\']", r"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("[","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("]","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(",",",:")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(" ","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace('"',"")
#Step 3 - TMAP - Remove the specific column names from TARGET_COLUMN_NAME
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_FILE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:EFFECTIVE_LOAD_DATE',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:START_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:END_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:ACTIVE_FL',"")
row2.to_csv("input/Coms_Temp_Join_Dont_Delete.csv", index=False,encoding='utf8')
#Step 4 - TMAP - Inner Join the Mapping Sheet with the YML Specifications Sheet
row3 = pd.read_csv('input/'+sys.argv[2]+'.csv',encoding='utf8')
row4 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete.csv',encoding='utf8')
out1 = pd.merge(row3, row4,
				on='TARGET_TABLE_NAME',
				how='inner')
#Step 5 - Renaming the Columns according to requirement
out1.rename(columns={"TARGET_TABLE_NAME": "TARGET_TABLE_NAME", "raw_raw_pipeline_name": "pipeline_name" ,"instance_name": "instance_name","bq.dataset": "bq_DATASET"},inplace=True)
out1.rename(columns={"gcs.project": "gcs_project", "bq.project": "bq_project" ,"bq.TempStorage": "bq_TempStorage","bq.dataset (Lower)": "bq_dataset"},inplace=True)
out1.rename(columns={" runtime_args:GCS_Input_Path(Bucket Name)": "runtime_args_GCS_Input_Path_Bucket_Name", " runtime_args:GCS_Input_Path(owner)": "runtime_args_GCS_Input_Path_owner" ,"bq.TempStorage": "bq_TempStorage"," runtime_args:GCS_Input_Path(Schema)": "runtime_args_GCS_Input_Path_Schema", " runtime_args: header": "runtime_args_header","bq.table": "bq_table", "Lower Table Name" : "Lower_Table_Name",'TARGET_COLUMN_NAME': 'header'},inplace=True)
#Step 6 - Generating Raw to Curation/ Raw to Raw file path
if out1["runtime_args_GCS_Input_Path_Schema"].empty:
	out1["encryptedfilepath"] = out1["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out1["runtime_args_GCS_Input_Path_owner"]+"/"+ out1["file_name"] +"/landing/*/*.bz2"
	out1["decryptedfilepath"] = out1["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out1["runtime_args_GCS_Input_Path_owner"]+"/"+ out1["file_name"] +"/processing/"
else:
	out1["encryptedfilepath"] = out1["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out1["runtime_args_GCS_Input_Path_owner"]+"/"+ out1["runtime_args_GCS_Input_Path_Schema"] + "/"+ out1["file_name"] +"/landing/*/*.bz2"
	out1["decryptedfilepath"] = out1["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out1["runtime_args_GCS_Input_Path_owner"]+"/"+ out1["runtime_args_GCS_Input_Path_Schema"] + "/"+ out1["file_name"] +"/processing/"
#Step 7 - Deleting unnecessary columns
#Common for both Raw to Raw and Raw to Curation
out1.drop('runtime_args_GCS_Input_Path_Bucket_Name', axis=1, inplace=True)
out1.drop('runtime_args_GCS_Input_Path_Schema', axis=1, inplace=True)
out1.drop('runtime_args_GCS_Input_Path_owner', axis=1, inplace=True)
out1.drop('runtime_args_header', axis=1, inplace=True)
out1.drop('bq_DATASET', axis=1, inplace=True)
out1.drop('Lower_Table_Name', axis=1, inplace=True)
# Only for Raw to Curation in case of RR - Replace line 42 - "raw_raw_pipeline_name": "pipeline_name"
# Replace with raw_curation_pipeline_name in case of raw to raw
out1.drop('raw_curation_pipeline_name', axis=1, inplace=True)
out1.to_csv("input/Coms1_Temp_Join_Dont_Delete1.csv", index=False, sep="|")
#Split
out2 = pd.read_csv('input/Coms1_Temp_Join_Dont_Delete1.csv', sep='|')
columnsTitles = ['TARGET_TABLE_NAME', 'pipeline_name', 'instance_name','location']
out2 = out2.reindex(columns=columnsTitles)
out2.to_csv('input/out/CSV_Yaml_Generated_1.csv', index=False,sep ='|')
#Split
out3 = pd.read_csv('input/Coms1_Temp_Join_Dont_Delete1.csv', sep='|')
out3.rename(columns={"file_name": "outputfileprefix"},inplace=True)
columnsTitles2 = ['TARGET_TABLE_NAME','encryptedfilepath','decryptedfilepath','gcs_project','outputfileprefix']
out3 = out3.reindex(columns=columnsTitles2)
out3.to_csv('input/out/CSV_Yaml_Generated_2.csv', index=False,sep ='|',encoding='utf8')
#Delete Temporary Files as Post Processing
os.remove(r'input/'+sys.argv[1]+'_temp1.csv')
os.remove(r'input/Coms_Temp_Join_Dont_Delete.csv')
os.remove(r'input/Coms1_Temp_Join_Dont_Delete1.csv')
os.system('python VF_HU_Yaml.py'+' '+'"'+sys.argv[2]+'"')

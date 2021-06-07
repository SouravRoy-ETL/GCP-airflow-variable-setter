# GCP-airflow-variable-setter
> Creating automated YML to set Variables in Apache Airflow (Composer) to reduce manual effort to 0. This activity has reduced resource focusing on Manual Tasks by 99%. 
Fault tolerance is 1% in case of wrong input passed by triggers. \n 1) Variable setter YML Structure for Landing GCP to Submission GCP \n2) Variable setter YML Sturcture for Submission GCP to DataLake GCP

# Structure to be created:

1) Submission GCP to DataLake GCP
pipeline: 
 pipeline_name: gcp_insert_append_1
 instance_name: bt-gmbh-nucl-dev-env-cdf-1
 location: us-east1
 runtime_args: {
  'GCS_Input_Path':'gs://bt-gmbh-nucl-dev/Schema_name/table_name/processing/*/table_name*',
  'header':'Id,:Zip,:City,:Street,:Country,:Building,:Door',
  'recipe':'table_name_recipe.txt',
  'bq.table':'table_name',
  'gcs.project':'bt-gmbh-nucl-dev',
  'bq.project':'bt-gmbh-nucl-dev',
  'bq.TempStorage':'bt-gmbh-nucl-dev-bq-tempstorage',
  'bq.dataset':'other_source_systems',
  'schema':'table_name_schema.txt'
 }

2) Landing GCP to Submission GCP
pipeline: 
 pipeline_name: gcp_insert_append_1
 instance_name: bt-gmbh-nucl-dev-env-cdf-1
 location: us-east1
 runtime_args: {
  'GCS_Input_Path':'gs://bt-gmbh-nucl-dev/Schema_name/table_name/processing/*/table_name*',
  'recipe':'table_name_recipe.txt',
  'gcs.project':'bt-gmbh-nucl-dev',
  'bq.project':'bt-gmbh-nucl-dev',
  'bq.dataset':'other_source_systems'
 }
 
# Installing
This Framework was created on **Python 3.8.5** and uses some external libraries listed below:

### a) YML, CSV
### b) Pandas
### c) Numpy

# Build/Run Command
Use following commands to build/Run the project from the project root. 
This script accepts 3 inputs and generates 2 YML Files
### Mapping Sheet (Excel File which has the Table_Name and Columns in rows)
### Sheet Name of the Above Excel sheet
### Config File which contains (bq.Table, bq.dataset, etc)
````
python .\Run_YML_Creater.py "Mapping_Sheet" "Excel_sheet_name" "Config_file_name"
````

### Authors
* Sourav Roy (souravroy7864@gmail.com)

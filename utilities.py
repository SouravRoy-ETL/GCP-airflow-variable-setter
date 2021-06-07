# DO NOT MODIFY THIS SCRIPT
import csv
import os
import sys
csvfile = open('input/out/CSV_Yaml_Generated_2.csv', 'r', encoding='utf-8')
datareader = csv.reader(csvfile, delimiter='|', quotechar='"')
data_headings = []
for row_index, row in enumerate(datareader):
	if row_index == 0:
		data_headings = row
	else:
		filename = 'temp/'+row[0].lower().replace(" county", "").replace(" ", "_") + '.yml'
		print(row[0])
		new_yaml = open(filename, 'a')
		yaml_text = ""
		yaml_text += " "
		yaml_text += "runtime_args: {\n"
		for cell_index, cell in enumerate(row):
			cell_heading = data_headings[cell_index].lower().replace(" ", "_").replace("-", "_").replace("%", "percent").replace("$", "").replace(",", "").replace("_",".")
			if "gcs.input.path" in cell_heading:
				cell_heading = data_headings[cell_index].replace(".", "_")
			if "bq.tempstorage" in cell_heading:
				cell_heading = data_headings[cell_index].replace("_", ".")
			cell_text = "'"+cell_heading+"'" + ":'" + cell.replace("\n", ", ") + "',\n"
			yaml_text += "  "+ cell_text
			if "schema" in cell_heading:
				cell_heading = data_headings[cell_index].replace(",", "")
		new_yaml.write(yaml_text + " }\n")
new_yaml.close()
csvfile.close()
os.system('python merge_yaml.py'+' '+'"'+sys.argv[1]+'"')

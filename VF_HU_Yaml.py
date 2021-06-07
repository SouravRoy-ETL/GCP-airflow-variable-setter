import csv
import os
import sys
csvfile = open('input/out/CSV_Yaml_Generated_1.csv', 'r',encoding="utf8")
datareader = csv.reader(csvfile, delimiter='|', quotechar='"')
data_headings = []
for row_index, row in enumerate(datareader):
	if row_index == 0:
		data_headings = row
	else:
		filename = 'temp/'+row[0].lower().replace(" county", "").replace(" ", "_") + '.yml'
		new_yaml = open(filename, 'a')
		yaml_text = ""
		yaml_text += "---\n"
		yaml_text += "pipeline: \n"
		for cell_index, cell in enumerate(row):
			cell_heading = data_headings[cell_index].lower().replace(" ", "_").replace("-", "_").replace("%", "percent").replace("$", "").replace(",", "")
			#cell_heading = data_headings[cell_index].replace("target_table_name","")
			cell_text =" "+ cell_heading + ": " + cell.replace("\n", ", ") + "\n"
			yaml_text += cell_text
		new_yaml.write(yaml_text + "")
new_yaml.close()
csvfile.close()
#call utilities for restructuring YML
os.system('python utilities.py'+' '+sys.argv[1])

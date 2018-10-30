import pandas as pd
import os
import csv

# scource_data_directory_path is the location of the directory holding all of the files
#       that need to be process.
source_data_directory_path = './tower_data'

# destination_csv_name is the file name that the processed data will be appended to, if it exists.
#       each subsequent run of this script will append everything in the source_data_directory_path to the
#           destination_csv_name file.
destination_csv_name = 'processed_data.csv'
files_to_import = os.listdir(source_data_directory_path)

for file in files_to_import:
    print('Importing Records from %s... ' % file)
    tower_id = file[8]
    with open(source_data_directory_path + '/' + file) as f:
        with open(destination_csv_name, "a", newline='') as temp_file:
            for line in f:
                if line is not None and line[0] == 'D':
                    row = line.split()
                    row.append(tower_id)
                    if row[5][0:3] == '3D6':
                        writer = csv.writer(temp_file, delimiter=',')
                        writer.writerow(row)
    # uncomment the line below to delete all of the files that have been processed.
    # os.remove(source_data_directory_path+'\\'+file)

data = pd.read_csv('./' + destination_csv_name,
                   names=['Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count', 'Gap', 'Tower'])
print(data.sample(n=10))

import pandas as pd
import os
import csv

# scource_data_directory_path is the location of the directory holding all of the files
#       that need to be process.
data_directory = './antenna_data/'
file_directories = [
    'Downstream',
    'Upstream 1',
    'Upstream 2',
    'Upstream 3'
]

# destination_csv_name is the file name that the processed data will be appended to, if it exists.
#       each subsequent run of this script will append everything in the source_data_directory_path to the
#           destination_csv_name file.
destination_csv_name = 'processed_data.csv'

processing_error_count = 0
reading_error_count = 0

# Remove processed data file if it exists
if os.path.exists(destination_csv_name):
    os.remove(destination_csv_name)

for dir_name in file_directories:
    file_path = data_directory + dir_name
    files_to_import = os.listdir(file_path)

    for file in files_to_import:
        print('Importing Records from %s... ' % file)

        tower_id = dir_name
        with open(file_path + '/' + file) as f:
            with open(destination_csv_name, "a", newline='') as temp_file:
                try:
                    for line in f:
                        if line is not None and line[0] == 'D':
                            row = line.split()
                            row.append(tower_id)
                            try:
                                if row[5][0:3] == '3D6':
                                    writer = csv.writer(temp_file, delimiter=',')
                                    writer.writerow(row)
                            except IndexError as e:
                                print('Error processing line: ' + line)
                                processing_error_count += 1
                except UnicodeDecodeError as e:
                    print('Error reading line: ' + line)
                    reading_error_count += 1


data = pd.read_csv('./' + destination_csv_name,
                   names=['Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count', 'Gap', 'Antenna'])
print(data.sample(n=10))


print('Process Complete.')
print('Error Reading %s records' % reading_error_count)
print('Error Processing %s records' % reading_error_count)

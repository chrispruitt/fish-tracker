import pandas as pd
import os
import csv
from datetime import datetime, time, timedelta

# scource_data_directory_path is the location of the directory holding all of the files
#       that need to be process.
data_directory = './antenna_data/'
file_directories = [
    'D1',
    'U1',
    'U2',
    'U3'
]

UPSTREAM_1_LAT = 33.99697222
UPSTREAM_1_LONG = -84.89694444
UPSTREAM_2_LAT = 33.99700000
UPSTREAM_2_LONG = -84.89805556
UPSTREAM_3_LAT = 33.99644444
UPSTREAM_3_LONG = -84.89944444
DOWNSTREAM_LAT = 33.99852778
DOWNSTREAM_LONG = -84.89444444

# destination_csv_name is the file name that the processed data will be appended to, if it exists.
#       each subsequent run of this script will append everything in the source_data_directory_path to the
#           destination_csv_name file.
destination_csv_name = 'processed_data.csv'

processing_error_count = 0
reading_error_count = 0
records = 0

# Remove processed data file if it exists
if os.path.exists(destination_csv_name):
    os.remove(destination_csv_name)

for dir_name in file_directories:
    file_path = data_directory + dir_name
    files_to_import = os.listdir(file_path)

    for file in files_to_import:
        print('Importing Records from %s... ' % file)

        antenna = dir_name
        with open(file_path + '/' + file, errors='ignore') as f:
            with open(destination_csv_name, "a", newline='') as temp_file:
                try:
                    for line in f:
                        if line is not None and line[0:2] == 'D ':
                            row = line.split()
                            try:
                                # check each column for validation
                                datetime.strptime(row[1], '%Y-%m-%d')
                                pd.to_timedelta(row[2])
                                pd.to_timedelta(row[3])
                                float(row[6])

                                if len(row) != 8:
                                    raise ValueError('Row should have 8 columns.')

                                if row[5][0:3] == '3D6':
                                    row.append(antenna)
                                    writer = csv.writer(temp_file, delimiter=',')
                                    writer.writerow(row)
                                    records += 1

                            except IndexError as e:
                                print('Error processing line: ' + line)
                                processing_error_count = processing_error_count + 1
                            except ValueError as e:
                                print('Error processing line: ' + line)
                                processing_error_count = processing_error_count + 1
                except UnicodeDecodeError as e:
                    print('Error reading line: ' + line)
                    reading_error_count += 1

print('Complete.')
print('Processed %s records.' % records)
print('Error reading %s records' % reading_error_count)
print('Error processing %s records' % processing_error_count)

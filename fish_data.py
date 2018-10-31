import pandas as pd
import os
import csv
from datetime import datetime

# scource_data_directory_path is the location of the directory holding all of the files
#       that need to be process.
data_directory = './antenna_data/'
file_directories = [
    'D1',
    'U1',
    'U2',
    'U3'
]

# UPSTREAM_1_LAT = 33.99697222
# UPSTREAM_1_LONG = -84.89694444
# UPSTREAM_2_LAT = 33.99700000
# UPSTREAM_2_LONG = -84.89805556
# UPSTREAM_3_LAT = 33.99644444
# UPSTREAM_3_LONG = -84.89944444
# DOWNSTREAM_LAT = 33.99852778
# DOWNSTREAM_LONG = -84.89444444

# destination_csv_name is the file name that the processed data will be appended to, if it exists.
#       each subsequent run of this script will append everything in the source_data_directory_path to the
#           destination_csv_name file.
destination_csv_name = 'processed_data.csv'

processing_error_count = 0
reading_error_count = 0
records = 0

tag_data = data = pd.read_csv('./tag_data.csv',
                              names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method ', 'Marked At'], low_memory=False)

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

                                #Round tag_time to nearest hour
                                row[2] = str(pd.to_timedelta(row[2]).round('H'))[7:]
                                pd.to_timedelta(row[3])
                                float(row[6])

                                species = tag_data.loc[tag_data['Tag ID'] == '3D6.00184CB8BF']['Species'].values[0]

                                if len(row) != 8:
                                    raise ValueError('Row should have 8 columns.')

                                if row[5][0:3] == '3D6':
                                    row.append(antenna)
                                    row.append(species)
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

<<<<<<< HEAD
=======

data = pd.read_csv('./' + destination_csv_name,
                   names=['Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count', 'Gap', 'Antenna', 'Species'], low_memory=False)

# Deduplicate by hour
data = data.drop_duplicates(subset=['Tag ID', 'Date', 'Time'])

data.to_csv('./' + destination_csv_name, sep=',', index=False, header=False)

print(data.sample(n=10))

>>>>>>> 415b1c8a22bf0f4854b6dcca508e87d9f51bc84f
print('Complete.')
print('Processed %s records.' % records)
print('Wrote %s records after deduplication.' % len(data.index))
print('Error reading %s records' % reading_error_count)
print('Error processing %s records' % processing_error_count)

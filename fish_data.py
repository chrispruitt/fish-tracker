import pandas as pd
import os
import csv
from datetime import datetime

start = datetime.now()

# scource_data_directory_path is the location of the directory holding all of the files
#       that need to be process.
data_directory = './data/antenna_data/'
# data_directory = './data/test_data/'
tag_data_path = './data/tag_data.csv'

file_directories = [name for name in os.listdir(data_directory) if os.path.isdir(os.path.join(data_directory, name))]

U1_LAT = 33.996972
U1_LONG = -84.896806
U2_LAT = 33.997
U2_LONG = -84.898167
U3_LAT = 33.996417
U3_LONG = -84.8995
D1_LAT = 33.998528
D1_LONG = -84.894528

# destination_csv_name is the file name that the processed data will be appended to, if it exists.
#       each subsequent run of this script will append everything in the source_data_directory_path to the
#           destination_csv_name file.
destination_csv_name = 'processed_data.csv'

processing_error_count = 0
reading_error_count = 0
records = 0
fish_list = []


# 6 hour intervals
time_rounding_list = [
    pd.to_timedelta('03:00:00'),
    pd.to_timedelta('09:00:00'),
    pd.to_timedelta('15:00:00'),
    pd.to_timedelta('21:00:00')
]
# time_rounding_list = [
#     pd.to_timedelta('01:00:00'),
#     pd.to_timedelta('02:00:00'),
#     pd.to_timedelta('03:00:00'),
#     pd.to_timedelta('04:00:00'),
#     pd.to_timedelta('05:00:00'),
#     pd.to_timedelta('06:00:00'),
#     pd.to_timedelta('07:00:00'),
#     pd.to_timedelta('08:00:00'),
#     pd.to_timedelta('09:00:00'),
#     pd.to_timedelta('10:00:00'),
#     pd.to_timedelta('11:00:00'),
#     pd.to_timedelta('12:00:00'),
#     pd.to_timedelta('13:00:00'),
#     pd.to_timedelta('14:00:00'),
#     pd.to_timedelta('15:00:00'),
#     pd.to_timedelta('16:00:00'),
#     pd.to_timedelta('17:00:00'),
#     pd.to_timedelta('18:00:00'),
#     pd.to_timedelta('19:00:00'),
#     pd.to_timedelta('20:00:00'),
#     pd.to_timedelta('21:00:00'),
#     pd.to_timedelta('22:00:00'),
#     pd.to_timedelta('23:00:00'),
#     pd.to_timedelta('24:00:00')
# ]


def round_to_nearest_time_in_list(time, time_list):

    min_timedelta = time_diff(time, time_list[0])
    rounded_time = time_list[0]

    for x in time_list:
        delta = time_diff(time, x)
        if delta <= min_timedelta:
            min_timedelta = delta
            rounded_time = x
    return rounded_time


def time_diff(x, y):
    if x > y:
        return x - y
    else:
        return y - x


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
            with open(destination_csv_name, "w", newline='') as temp_file:
                try:
                    for line in f:
                        if line is not None and line[0:2] == 'D ':
                            row = line.split()
                            try:
                                # check each column for validation
                                datetime.strptime(row[1], '%Y-%m-%d')

                                # Round tag_time to nearest time in list
                                time_of_day = str(pd.to_timedelta(row[2]).round('H'))
                                row[2] = str(round_to_nearest_time_in_list(pd.to_timedelta(time_of_day), time_rounding_list))[7:]
                                pd.to_timedelta(row[3])
                                float(row[6])

                                if len(row) != 8:
                                    raise ValueError('Row should have 8 columns.')

                                if row[5][0:3] == '3D6':
                                    row.append(antenna)
                                    fish_list.append(row)
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

fish_data = pd.DataFrame(fish_list,
                         columns=['D', 'Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count', 'Gap', 'Antenna'])

print('\nDeduplicating records...')
# Deduplicate by hour
fish_data = fish_data.drop_duplicates(subset=['Tag ID', 'Date', 'Time', 'Antenna'])

print('Appending location data...')
fish_data.loc[fish_data.Antenna == 'U1', 'Lat'] = U1_LAT
fish_data.loc[fish_data.Antenna == 'U1', 'Long'] = U1_LONG
fish_data.loc[fish_data.Antenna == 'U2', 'Lat'] = U2_LAT
fish_data.loc[fish_data.Antenna == 'U2', 'Long'] = U2_LONG
fish_data.loc[fish_data.Antenna == 'U3', 'Lat'] = U3_LAT
fish_data.loc[fish_data.Antenna == 'U3', 'Long'] = U3_LONG
fish_data.loc[fish_data.Antenna == 'D1', 'Lat'] = D1_LAT
fish_data.loc[fish_data.Antenna == 'D1', 'Long'] = D1_LONG

print('Merging species data...')
# Load fish tag data into a dataframe
fish_tag_data = pd.read_csv(tag_data_path,
                            names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                            low_memory=False)

# Join Dataframe on Tag ID
fish_data = pd.merge(fish_data, fish_tag_data[['Tag ID', 'Species', 'Length', 'Marked At']], on='Tag ID')

# Convert 'Date' to datetime type and derive the Week Number as 'Week'
fish_data['Date'] = fish_data['Date'].astype('datetime64[ns]')
fish_data['Week'] = fish_data['Date'].dt.week

# Was the tagged fish present at the indicated antenna 0/No, 1/Yes
fish_data['D1'] = fish_data['Antenna'].isin(['D1'])
fish_data['U1'] = fish_data['Antenna'].isin(['U1'])
fish_data['U2'] = fish_data['Antenna'].isin(['U2'])
fish_data['U3'] = fish_data['Antenna'].isin(['U3'])
fish_data.D1 = fish_data.D1.astype(int)
fish_data.U1 = fish_data.U1.astype(int)
fish_data.U2 = fish_data.U2.astype(int)
fish_data.U3 = fish_data.U3.astype(int)

# Fill all missing values in DataFrame with zero
fish_data = fish_data.fillna(value=0)

print('Sorting data by date and time')
fish_data = fish_data.sort_values(['Date', 'Time'])

print('Writing to csv...\n')
fish_data.to_csv('./processed_data.csv', header=['D', 'Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count',
                 'Gap', 'Antenna', 'Lat', 'Long', 'Species', 'Length', 'Marked At', 'Week',
                 'D1', 'U1', 'U2', 'U3'],
                 index=False)

print(fish_data.sample(n=10))

end = datetime.now()
duration = end - start
duration = divmod(duration.total_seconds(), 60)

print('\nCompleted in %d mins and %d seconds.' % (int(round(duration[0])), int(round(duration[1]))))
print('Processed %s records.' % records)
print('Wrote %s records after deduplication.' % len(fish_data.index))
print('Error reading %s records' % reading_error_count)
print('Error processing %s records' % processing_error_count)

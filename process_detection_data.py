import pandas as pd
import os
from datetime import datetime, timedelta
import argparse

###########################################################################################
"""
Description:
This Script is used to cleanse data for antenna detections.
"""

# PARAMETERS:
parser = argparse.ArgumentParser()
parser.add_argument("-sh", "--start_hour", help="hour of the day to start rounding detection times", type=int, default=0)
parser.add_argument("-i", "--interval", help="interval in which to round detection times", type=int, default=1)
args = parser.parse_args()
start_hour = args.start_hour
interval = args.interval

# PATHS:
data_directory = './data/antenna_data/'
# data_directory = './data/test_data/'
tag_data_path = './data/tag_data.csv'
destination_csv_name = './results/cleansed_detection_data.csv'

###########################################################################################

start_timer = datetime.now()


def get_file_directories(data_directory):
    return [name for name in os.listdir(data_directory) if os.path.isdir(os.path.join(data_directory, name))]


def create_time_rounding_list(start_hour, interval):
    time_rounding_list = []
    print('Detection times will be rounded to the nearest time of day in the following list:')
    while start_hour <= 23:
        td = timedelta(hours=start_hour)
        print(td)
        time_rounding_list.append(pd.to_timedelta(str(timedelta(hours=start_hour))))
        start_hour += interval
    print('')
    return time_rounding_list


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


def get_detections_from_master_list_file():
    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 header=0,
                                 low_memory=False)

    detections = []
    for index, row in master_list_df.iterrows():
        detections.append({
            'D': 'D',
            'Date': datetime.strptime(row['Date'], '%m/%d/%Y').strftime('%Y-%m-%d'),
            'Time': '00:00:00',
            'Tag ID': row['Tag ID'],
            'Antenna': str(row['Marked At'].strip())[:2]
        })

    return detections
    # return pd.DataFrame.from_dict(detections, orient='columns')

# Detection Data
# D,Date,Time,Tag ID,Antenna,Species,Length,Marked At
# D,2018-04-16,00:00:00,3D6.00184CBA4E,U1,LEAU,99,U1.0

# 2372

# Master List
# Date,Time,Tag ID,Species,Length,Capture Method,Marked At
# 2017-07-14,11:00:00,3D6.00184CE0C9,HYET,98,Fyke,D1


def main(start_hour, interval):
    global data_directory, tag_data_path, destination_csv_name

    time_rounding_list = create_time_rounding_list(start_hour, interval)
    file_directories = get_file_directories(data_directory)

    processing_error_count = 0
    reading_error_count = 0
    records = 0
    fish_list = []

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
                                    dt = datetime.strptime(row[1], '%Y-%m-%d')
                                    # row[1] = dt - timedelta(days=dt.weekday())

                                    # Round tag_time to nearest time in list
                                    time_of_day = str(pd.to_timedelta(row[2]).round('H'))
                                    row[2] = str(round_to_nearest_time_in_list(pd.to_timedelta(time_of_day), time_rounding_list))[7:]
                                    # row[2] = "00:00:00"

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

    # Drop unwanted columns
    fish_data = fish_data.drop(axis='columns', columns=['Duration', 'Type', 'Gap', 'Count'])

    print('\nDeduplicating records...')
    # Deduplicate by hour


    # Add initial detections from Marked At data in master list
    #initial_detections = pd.DataFrame.from_dict(get_detections_from_master_list_file())
    #print(initial_detections.sample(n=1))
    #print(fish_data.sample(n=1))
    #ifish_data = fish_data.append(initial_detections)
    # # print(initial_detections.sample(n=10))
    #
    # fish_array = fish_data.to_dict(orient="records")
    # print(fish_array[0])
    # print(initial_detections[0])
    # fish_array.append(initial_detections)
    # # frames = [initial_detections, fish_data]
    # # fish_data = pd.concat(frames)
    # # print(fish_array)
    # fish_data = pd.DataFrame.from_dict(fish_array)

    # Drop duplicate detections
    fish_data = fish_data.drop_duplicates(subset=['Tag ID', 'Date', 'Time', 'Antenna'])

    print('Merging species data...')
    # Load fish tag data into a dataframe
    fish_tag_data = pd.read_csv(tag_data_path,
                                names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                low_memory=False)

    # Join Dataframe on Tag ID
    fish_data = pd.merge(fish_data, fish_tag_data[['Tag ID', 'Species', 'Length', 'Marked At']], on='Tag ID')

    # Fill all missing values in DataFrame with zero
    fish_data = fish_data.fillna(value=0)

    print('Sorting data by date and time')
    fish_data = fish_data.sort_values(['Date', 'Time'])

    print('Writing to csv...\n')
    fish_data.to_csv(destination_csv_name, header=['D', 'Date', 'Time', 'Tag ID', 'Antenna', 'Species', 'Length', 'Marked At'],
                     index=False)

    print(fish_data.sample(n=10))

    print('\nProcessed %s records.' % records)
    print('Wrote %s records after deduplication.' % len(fish_data.index))
    print('Error reading %s records' % reading_error_count)
    print('Error processing %s records' % processing_error_count)


main(start_hour, interval)

end_timer = datetime.now()
duration = end_timer - start_timer
duration = divmod(duration.total_seconds(), 60)
print('\nCompleted in %d mins and %d seconds.' % (int(round(duration[0])), int(round(duration[1]))))

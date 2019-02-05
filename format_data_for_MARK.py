import pandas as pd, csv
from datetime import datetime, timedelta
import argparse

###########################################################################################
"""
Description:
This Script is used to create a file formated for the program MARK
"""

# PARAMETERS
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start_date", help="start date to start matching detections. Example: 2018-09-19")
parser.add_argument("-e", "--end_date", help="end date to stop matching detection. Example: 2018-09-19")
parser.add_argument("-sh", "--start_hour", help="hour of the day to start matching detection times", default=3, type=int)
parser.add_argument("-i", "--interval", help="interval in hours to match detections times", default=6, type=int)
args = parser.parse_args()
start_datetime = datetime.strptime(args.start_date, '%Y-%m-%d') + timedelta(hours=args.start_hour)
end_datetime = datetime.strptime(args.end_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
interval_hours = args.interval

# PATHS:
optimal_dates_path = './data/optimal_dates.csv'  # only the dates in this file will be used to record data. all other dates will be skipped.
tag_data_path = './results/cleansed_master_list.csv'
processed_data_path = './results/cleansed_detection_data.csv'
destination_csv_name = './results/mark.txt'

###########################################################################################

start_timer = datetime.now()

# If error on write, toggle this. I know its stupid, but don't have time to explain.
# quoting = csv.QUOTE_NONE
# quoting = csv.QUOTE_MINIMAL


def main():
    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 header=0,
                                 low_memory=False)

    optimal_dates_df = pd.read_csv(optimal_dates_path,
                                 names=['Date'],
                                 low_memory=False)

    detection_df = pd.read_csv(processed_data_path,
                               names=['D', 'Date', 'Time', 'Tag ID', 'Antenna', 'Species', 'Length', 'Marked At'],
                               low_memory=False)

    # TODO: mark first encounter when captured

    master_list_df['Encounter History'] = ''

    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        if not optimal_dates_df.loc[(optimal_dates_df['Date'] == current_datetime.strftime('%Y-%m-%d'))].empty:

            for index, row in master_list_df.iterrows():
                try:
                    tag_id = row[2]
                    marked_datetime = datetime.strptime(row[0], '%Y-%m-%d') + pd.to_timedelta(row[1])
                    marked_at_antenna = row[6]
                except Exception as e:
                    print('Error processing row:', row)
                    print(e)
                if current_datetime >= marked_datetime:
                    result = detection_df.loc[(detection_df['Tag ID'] == tag_id) & (detection_df['Date'] == current_datetime.strftime('%Y-%m-%d')) & (detection_df['Time'] == current_datetime.strftime('%H:%M:%S'))]
                    if result.empty:
                        master_list_df.at[index, 'Encounter History'] = row[7] + '0'
                    else:
                        if result.shape[0] > 1:
                            print('Mutliple Detections of with tag id: %s ' % tag_id)
                            print(result)

                        antenna = ''
                        # get first row in result
                        for result_index, result_row in result.iterrows():
                            antenna = result_row['Antenna']
                            break
                        if antenna == 'D1':
                            antenna = '1'
                        elif antenna == 'U1':
                            antenna = '2'
                        elif antenna == 'U2':
                            antenna = '3'
                        elif antenna == 'U3':
                            antenna = '4'
                        else:
                            antenna = 'X'
                        master_list_df.at[index, 'Encounter History'] = row[7] + antenna
                else:
                    master_list_df.at[index, 'Encounter History'] = row[7] + '.'

            current_datetime = current_datetime + timedelta(hours=interval_hours)
        else:
            print('Skipping Date:', current_datetime.strftime('%Y-%m-%d'))
            current_datetime = current_datetime + timedelta(days=1)

    master_list_df = master_list_df.drop(axis='columns', columns=['Date', 'Time', 'Species', 'Length', 'Capture Method', 'Marked At'])
    master_list_df['Col_1'] = '/*'
    master_list_df['Col_3'] = '*/'
    master_list_df['Col_5'] = '1;'

    master_list_df = master_list_df[['Col_1', 'Tag ID', 'Col_3', 'Encounter History', 'Col_5']]

    print(master_list_df.sample(n=1))


    master_list_df.to_csv(destination_csv_name,
                          sep=' ',
                          index=False,
                          header=False,
                          quoting=csv.QUOTE_NONE)


main()

end_timer = datetime.now()
duration = end_timer - start_timer
duration = divmod(duration.total_seconds(), 60)
print('\nCompleted in %d mins and %d seconds.' % (int(round(duration[0])), int(round(duration[1]))))

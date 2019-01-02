# parameters (start_datetime, end_datetime, interval(in hours))

import pandas as pd
from datetime import datetime, timedelta

tag_data_path = './data/tag_data.csv'
processed_data_path = './processed_data.csv'

start_date_time = datetime(year=2018, month=10, day=1, hour=3)
end_date_time = datetime(year=2018, month=10, day=15, hour=23, minute=59)
interval_hours = 6


def read_time_to_timedelta(timeString):
    time = timeString.split(':')
    hours = int(time[0])
    minutes = int(time[1].split()[0])
    if time[1].split()[1] == 'PM' and hours != 12:
        hours += 12
    return timedelta(hours=hours, minutes=minutes)


def main():
    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 low_memory=False)

    detection_df = pd.read_csv(processed_data_path,
                               names=['D', 'Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count',
                                      'Gap', 'Antenna', 'Lat', 'Long', 'Species', 'Length', 'Marked At', 'Week',
                                      'D1', 'U1', 'U2', 'U3'],
                               low_memory=False)

    master_list_df['encounter_history'] = ''

    current_date = start_date_time
    while current_date <= end_date_time:
        # print(current_date)
        # for each tag in master list
        for index, row in master_list_df.iterrows():
            try:
                tag_id = row[2]
                marked_datetime = datetime.strptime(row[0], '%m/%d/%Y') + read_time_to_timedelta(row[1])
                marked_at_antenna = str(row[6])[:2]
                print(marked_datetime)

            except Exception as e:
                print('Error processing row:', row)
                print(e)
            # if marked date is after or equal to time
                # if tag exists for time in detection_df
                    # add 1,2,3,or 4 depending on the antenna to encounter_history
                # else
                    # add zero to encounter_history string
            # else
                # add dot to encounter_history
        current_date = current_date + timedelta(hours=interval_hours)

    print(master_list_df.sample(n=2))

    # drop columns not needed


main()
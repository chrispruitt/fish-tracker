import pandas as pd
import os
from datetime import datetime, timedelta


tag_data_path = './data/tag_data.csv'
destination_csv_name = './results/cleaned_master_list.csv'

column_names = ['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At']


def read_time_to_timedelta(time_string):
    time = time_string.split(':')
    hours = int(time[0])
    minutes = int(time[1].split()[0])
    if time[1].split()[1] == 'PM' and hours != 12:
        hours += 12
    return timedelta(hours=hours, minutes=minutes)


def main():
    if os.path.exists(destination_csv_name):
        os.remove(destination_csv_name)

    master_list_df = pd.read_csv(tag_data_path,
                                 names=column_names,
                                 low_memory=False)

    master_fish_array = []
    ignored_rows = 0
    for index, row in master_list_df.iterrows():
        # create current location dict for each tagged fish

        try:
            master_fish_array.append({
                'Date': datetime.strptime(row[0], '%m/%d/%Y').strftime('%Y-%m-%d'),
                'Time': str(read_time_to_timedelta(row[1])),
                'Tag ID': row[2],
                'Species': row[3],
                'Length': row[4],
                'Capture Method': row[5],
                'Marked At': str(row[6])[:2]
            })

        except Exception as e:
            print('Ignoring Row:', row)
            print(e)
            ignored_rows += 1

    clean_master_list = pd.DataFrame.from_dict(master_fish_array, orient='columns')[column_names]

    clean_master_list.to_csv(destination_csv_name,
                             index=False)

    print(clean_master_list.sample(n=10))

    print('Ignored %s rows' % ignored_rows)
    print('Completed.')


main()

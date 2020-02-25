import pandas as pd
import os
from datetime import datetime
import sys
###########################################################################################
"""
Description:
This Script is used to record every migration a tag has given a data set.
"""

# PARAMETERS:
tag_data_path = './results/cleansed_master_list.csv'
processed_data_path = './results/cleansed_detection_data.csv'
destination_csv_name = './results/migrations.csv'

###########################################################################################

start_timer = datetime.now()

migration_columns = ['tag_id',
                     'prev_loc',
                     'prev_loc_first_date',
                     'prev_loc_first_time',
                     'prev_loc_last_date',
                     'prev_loc_last_time',
                     'new_loc',
                     'new_loc_first_date',
                     'new_loc_first_time',
                     'new_loc_last_date',
                     'new_loc_last_time',
                     'species',
                     'length']


def get_location(loc_df, tag_id):
    df = loc_df.loc[loc_df['tag_id'] == tag_id].to_dict(orient='records')
    if df is not None and df != []:
        return df[0]
    else:
        return None


def update_datetime(loc_df, tag_id, date, time):
    loc_df.loc[loc_df['tag_id'] == tag_id, 'date'] = date
    loc_df.loc[loc_df['tag_id'] == tag_id, 'time'] = time


def update_location_datetime(loc_df, tag_id, antenna, date, time):
    loc_df.loc[loc_df['tag_id'] == tag_id, 'antenna'] = antenna
    loc_df.loc[loc_df['tag_id'] == tag_id, 'first_date'] = date
    loc_df.loc[loc_df['tag_id'] == tag_id, 'first_time'] = time
    loc_df.loc[loc_df['tag_id'] == tag_id, 'date'] = date
    loc_df.loc[loc_df['tag_id'] == tag_id, 'time'] = time


def update_last_detection_datetime(migrations_df, tag_id, date, time):
    migrations_df.loc[(migrations_df['tag_id'] == tag_id) & (migrations_df['new_loc_last_date'].isnull()), 'new_loc_last_date'] = date
    migrations_df.loc[(migrations_df['tag_id'] == tag_id) & (migrations_df['new_loc_last_time'].isnull()), 'new_loc_last_time'] = time


def main():

    if os.path.exists(destination_csv_name):
        os.remove(destination_csv_name)

    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 low_memory=False,
                                 header=0)

    detection_df = pd.read_csv(processed_data_path,
                               names=['D', 'Date', 'Time', 'Tag ID', 'Antenna', 'Species', 'Length', 'Marked At'],
                               low_memory=False,
                               header=0)

    master_fish_array = []
    ignored_rows = 0
    for index, row in master_list_df.iterrows():
        try:
            master_fish_array.append({
                'tag_id': row["Tag ID"],
                'antenna': row["Marked At"],
                'first_date': row["Date"],
                'first_time': row["Time"],
                'date': row["Date"],
                'time': row["Time"],
                'species': row["Species"],
                'length': row["Length"]
            })

        except Exception as e:
            print('ignoring row')
            ignored_rows += 1
            print(e)

    loc_df = pd.DataFrame.from_dict(master_fish_array, orient='columns')

    migrations_df = pd.DataFrame(columns=migration_columns)

#D,Date,Time,Tag ID,Antenna,Species,Length,Marked At
#D,2018-04-17,12:00:00,3D6.00184CBA4E,U1,LEAU,99,U1.0

    for index, row in detection_df.iterrows():
        try:
            tag_id = row["Tag ID"]
            date = row["Date"]
            time = row["Time"]
            antenna = row["Antenna"]

            # print(date)
            # TODO: Make an argument to start migrations from date.
            if datetime.strptime(date, '%Y-%m-%d') >= datetime.strptime('2018-07-01', '%Y-%m-%d'):
                current_location = get_location(loc_df, tag_id)
                if current_location is not None:
                    if antenna != current_location['antenna']:
                        last_date = current_location['date']
                        last_time = current_location['time']
                        dict = {
                            'tag_id': tag_id,
                            'prev_loc': current_location['antenna'],
                            'prev_loc_first_date': current_location['first_date'],
                            'prev_loc_first_time': current_location['first_time'],
                            'prev_loc_last_date': current_location['date'],
                            'prev_loc_last_time': current_location['time'],
                            'new_loc': antenna,
                            'new_loc_first_date': date,
                            'new_loc_first_time': time,
                            'species': current_location['species'],
                            'length': current_location['length']
                        }

                        update_last_detection_datetime(migrations_df, tag_id, last_date, last_time)

                        migrations_df = migrations_df.append(dict, ignore_index=True)

                        update_location_datetime(loc_df, tag_id, antenna, date, time)
                    else:
                        update_datetime(loc_df, tag_id, date, time)
                else:
                    print('Tag with id: %s does not exist in master list.' % tag_id)

        except Exception as e:
            print(e)

    for index, row in loc_df.iterrows():
        tag_id = row["tag_id"]
        last_date = loc_df.loc[loc_df['tag_id'] == tag_id, "date"].values[0]
        last_time = loc_df.loc[loc_df['tag_id'] == tag_id, "time"].values[0]
        update_last_detection_datetime(migrations_df, tag_id, last_date, last_time)

    print(migrations_df.sample(n=2))

    migrations_df.to_csv(destination_csv_name,
                         header=migration_columns,
                         index=False)

    print('Ignored %s rows' % ignored_rows)
    sys.exit(0)


main()

end_timer = datetime.now()
duration = end_timer - start_timer
duration = divmod(duration.total_seconds(), 60)
print('\nCompleted in %d mins and %d seconds.' % (int(round(duration[0])), int(round(duration[1]))))

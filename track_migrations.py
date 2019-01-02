import pandas as pd
import os

tag_data_path = './clean_master_list.csv'
processed_data_path = './processed_data.csv'
destination_csv_name = './migrations.csv'


def get_location(loc_pd, tag_id):
    df = loc_pd.loc[loc_pd['tag_id'] == tag_id].to_dict(orient='records')
    if df is not None and df != []:
        return df[0]
    else:
        return None


def update_current_location(loc_pd, tag_id, antenna, date, time):
    loc_pd.loc[loc_pd['tag_id'] == tag_id, 'antenna'] = antenna
    loc_pd.loc[loc_pd['tag_id'] == tag_id, 'date'] = date
    loc_pd.loc[loc_pd['tag_id'] == tag_id, 'time'] = time


def main():

    if os.path.exists(destination_csv_name):
        os.remove(destination_csv_name)

    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 low_memory=False)

    detection_df = pd.read_csv(processed_data_path,
                                 names=['D', 'Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count',
                                               'Gap', 'Antenna', 'Lat', 'Long', 'Species', 'Length', 'Marked At', 'Week',
                                               'D1', 'U1', 'U2', 'U3'],
                                 low_memory=False)

    master_fish_array = []
    ignored_rows = 0
    for index, row in master_list_df.iterrows():
        # create current location dict for each tagged fish

        try:
            master_fish_array.append({
                'tag_id': row[2],
                'antenna': row[6],
                'date': row[0],
                'time': row[1]
            })

        except Exception as e:
            print('ignoring row')
            ignored_rows += 1
            print(e)

    loc_pd = pd.DataFrame.from_dict(master_fish_array, orient='columns')

    columns = ['tag_id', 'prev_loc', 'prev_loc_date', 'prev_loc_time', 'new_loc', 'new_loc_date', 'new_loc_time']
    migrations_df = pd.DataFrame(columns=columns)

    for index, row in detection_df.iterrows():
        try:
            tag_id = row[5]
            date = row[1]
            time = row[2]
            antenna = row[8]
            current_location = get_location(loc_pd, row[5])
            if current_location is not None:
                if antenna != current_location['antenna']:
                    dict = {
                        'tag_id': tag_id,
                        'prev_loc': current_location['antenna'],
                        'prev_loc_date': current_location['date'],
                        'prev_loc_time': current_location['time'],
                        'new_loc': antenna,
                        'new_loc_date': date,
                        'new_loc_time': time
                    }
                    print(dict)

                    migrations_df = migrations_df.append(dict, ignore_index=True)

                update_current_location(loc_pd, tag_id, antenna, date, time)

            else:
                print('Tag with id: %s does not exist in master list.' % tag_id)

        except Exception as e:
            print(e)

    print(migrations_df.sample(n=2))

    migrations_df.to_csv(destination_csv_name,
                         header=['tag_id', 'prev_loc', 'prev_loc_date', 'prev_loc_time',
                                 'new_loc', 'new_loc_date', 'new_loc_time'],
                         index=False)

    print('Ignored %s rows' % ignored_rows)


main()

# parameters (start_date, end_date, timedelta(by day or by hour))

import pandas as pd

tag_data_path = './data/tag_data.csv'
processed_data_path = './processed_data.csv'

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


def main():
    master_list_df = pd.read_csv(tag_data_path,
                                 names=['Date', 'Time', 'Tag ID', 'Species', 'Length', 'Capture Method', 'Marked At'],
                                 low_memory=False)

    # detection_df = pd.read_csv(processed_data_path,
    #                            names=['D', 'Date', 'Time', 'Duration', 'Type', 'Tag ID', 'Count',
    #                                   'Gap', 'Antenna', 'Lat', 'Long', 'Species', 'Length', 'Marked At', 'Week',
    #                                   'D1', 'U1', 'U2', 'U3'],
    #                            low_memory=False)

    master_list_df['encounter_history'] = ''

    # for start_time to end_time
        # for each tag in master list
            # if marked date is after or equal to time
                # if tag exists for time in detection_df
                    # add 1,2,3,or 4 depending on the antenna to encounter_history
                # else
                    # add zero to encounter_history string
            # else
                # add dot to encounter_history


    print(master_list_df.sample(n=2))

    # drop columns not needed


main()
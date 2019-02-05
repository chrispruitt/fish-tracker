#!/usr/bin/env bash

python3 clean_master_list.py

python3 process_detection_data.py --start_hour 3 --interval 6
python3 format_data_for_MARK.py --start_date 2018-09-19 --end_date 2018-09-29 --start_hour 3 --interval 6

#python3 process_detection_data.py --start_hour 0 --interval 1
#python3 track_migrations.py

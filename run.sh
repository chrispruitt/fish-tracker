#!/usr/bin/env bash

python3 clean_master_list.py

START_HOUR=3
INTERVAL=6
python3 process_detection_data.py --start_hour ${START_HOUR} --interval ${INTERVAL}
python3 format_data_for_MARK.py --start_date 2018-07-02 --end_date 2018-11-14 --start_hour ${START_HOUR} --interval ${INTERVAL}


START_HOUR=0
INTERVAL=1
python3 process_detection_data.py ${START_HOUR} --interval ${INTERVAL}
python3 track_migrations.py

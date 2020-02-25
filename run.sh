#!/usr/bin/env bash

python3.6 clean_master_list.py

#START_HOUR=0
#INTERVAL=168
#python3.6 process_detection_data.py --start_hour ${START_HOUR} --interval ${INTERVAL}
#python3.6 format_data_for_MARK.py --start_date 2018-07-02 --end_date 2018-11-05 --start_hour ${START_HOUR} --interval ${INTERVAL}


START_HOUR=0
INTERVAL=1
python3.6 process_detection_data.py --start_hour ${START_HOUR} --interval ${INTERVAL}
python3.6 track_migrations.py

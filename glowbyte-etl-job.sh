#!/bin/bash
source /home/edd-ign/Code/Projects/glowbyte-hack-2022/glowbyte/bin/activate
python /home/edd-ign/Code/Projects/glowbyte-hack-2022/dwh_etl.py &&
python /home/edd-ign/Code/Projects/glowbyte-hack-2022/update_data_marts.py

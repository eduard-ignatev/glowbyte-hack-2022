import os
import pandas as pd

from dotenv import load_dotenv
from ftplib import FTP_TLS
from sqlalchemy import create_engine
from loguru import logger

# Загружаем credentials из переменных окружения
load_dotenv()
SOURCE_DB_URI = os.environ['SOURCE_DB_URI']
SOURCE_FTP_HOST = os.environ['SOURCE_FTP_HOST']
SOURCE_FTP_USER = os.environ['SOURCE_FTP_USER']
SOURCE_FTP_PWD = os.environ['SOURCE_FTP_PWD']
DWH_DB_URI = os.environ['DWH_DB_URI']

# --------------------------------------------------------
# Incremental data load
# --------------------------------------------------------


# TO-DO (NOT TESTED)

# Создаем движок для соединения с БД источником
logger.info('Querying data from source DB...')
source_db_conn = create_engine(SOURCE_DB_URI, connect_args={'sslmode': 'require'})

# Создаем движок для соединения с БД хранилища данных
logger.info('Loading data to DWH...')
dwh_db_conn = create_engine(DWH_DB_URI, connect_args={'sslmode': 'require'})

# Get last load date
query = """
    SELECT COALESCE(max(bd.loaded_until),
        TO_DATE('01-01-1900', 'MM-DD-YYYY'))
    FROM dwh_kazan.work_batchdate AS bd
    WHERE bd.status = 'Success'
"""
last_load_time_t = pd.read_sql(query, dwh_db_conn)
ts = pd.to_datetime(last_load_time_t.values[0])
last_load_time = ts.strftime('%Y.%m.%d')[0]

###############
# dim_cars table

# Get car_pool updates
query = f"""
    SELECT plate_num,
        update_dt AS start_dt,
        model AS model_name,
        revision_dt,
        '9999-01-01 00:00:00' AS end_dt
    FROM main.car_pool
    WHERE CAST(update_dt AS DATE) - 1 = '{last_load_time}'
"""
updated_cars = pd.read_sql(query, source_db_conn)
updated_cars.plate_num = updated_cars.plate_num.str.strip()

# Upload car_pool updates to temp_table
updated_cars.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

# Update end_dt for previously loaded rows
query = """
    UPDATE dwh_kazan.dim_cars AS f
    SET end_dt = t.start_dt - INTERVAL '1' second
    FROM dwh_kazan.work_temp_table AS t
    WHERE f.plate_num = t.plate_num
        AND f.end_dt = '9999-01-01 00:00:00'
"""
with dwh_db_conn.begin() as conn:
    conn.execute(query)

# Upload car_pool updates to target table
updated_cars.to_sql('dim_cars', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

#################
# dim_drivers

# Get drivers updates
query = f"""
    SELECT 
        md5(last_name || first_name || middle_name || birth_dt) AS personnel_num,
        update_dt AS start_dt,
        last_name,
        first_name,
        middle_name,
        birth_dt,
        card_num,
        driver_license AS driver_license_num,
        driver_valid_to AS driver_license_dt,
        '9999-01-01 00:00:00' AS end_dt
    FROM main.drivers
    WHERE CAST(update_dt AS DATE) - 1 = '{last_load_time}'
"""
updated_drivers = pd.read_sql(query, source_db_conn)

# Upload drivers updates to temp_table
updated_drivers.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

# Update end_dt for previously loaded rows
query = """
    UPDATE dwh_kazan.dim_drivers AS f
    SET end_dt = t.start_dt - INTERVAL '1' second
    FROM dwh_kazan.work_temp_table AS t
    WHERE f.personnel_num = t.personnel_num
        AND f.end_dt = '9999-01-01 00:00:00'
"""
with dwh_db_conn.begin() as conn:
    conn.execute(query)

# Upload drivers updates to target table
updated_drivers.to_sql('dim_drivers', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

#################
# dim_clients

# Get clients updates
query = f"""
    SELECT
        client_phone AS phone_num,
        dt AS start_dt,
        card_num,
        LEAD (dt, 1, '9999-01-01 00:00:01') OVER
            (
                PARTITION BY client_phone ORDER BY dt
            ) - INTERVAL '1 second' AS end_dt
    FROM
    (
        SELECT 
            client_phone,
            card_num,
            MIN(dt) AS dt
        FROM main.rides
        GROUP BY client_phone, card_num
    ) AS cards
    WHERE CAST(dt AS DATE) - 1 = '{last_load_time}'
"""
updated_clients = pd.read_sql(query, source_db_conn)

# Create work dim_clients table for managing duplicates
dim_clients = pd.read_sql("SELECT * FROM dwh_kazan.dim_clients", dwh_db_conn)
dim_clients.to_sql('work_dim_clients_copy', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

# Upload drivers updates to temp_table
updated_clients.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

# Update end_dt for previously loaded rows
query = """
    UPDATE dwh_kazan.dim_clients AS f
    SET end_dt = t.start_dt - INTERVAL '1' second
    FROM dwh_kazan.work_temp_table AS t
    WHERE f.phone_num = t.phone_num
        AND f.end_dt = '9999-01-01 00:00:00'
        AND t.card_num != f.card_num
"""
with dwh_db_conn.begin() as conn:
    conn.execute(query)

# Upload clients updates to work_dim_clients table
updated_clients.to_sql('work_dim_clients_copy', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

# Deduplicate work_dim_clients table and get row to append to target table
query = f"""
    SELECT 
        phone_num,
        MIN(start_dt) AS start_dt,
        card_num,
        deleted_flag,
        end_dt
    FROM dwh_kazan.work_dim_clients_copy
    GROUP BY phone_num, card_num, deleted_flag, end_dt
    HAVING CAST(MIN(start_dt) AS DATE) - 1 = '{last_load_time}'
    ORDER BY 2
"""
final_clients = pd.read_sql(query, dwh_db_conn)
final_clients.iloc[:, 3] = "N"

# Upload clients updates to target table
final_clients.to_sql('dim_clients', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')


# Remove work tables and add script execution timestamp
with dwh_db_conn.begin() as conn:
    conn.execute("DROP TABLE dwh_kazan.work_temp_table")
    conn.execute("DROP TABLE dwh_kazan.work_dim_clients_copy")
    conn.execute(f"INSERT INTO dwh_kazan.work_batchdate (loaded_until, status) VALUES('date(now)', 'Success')")
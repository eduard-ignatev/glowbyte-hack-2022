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
source_db_conn = create_engine(SOURCE_DB_URI)

# Создаем движок для соединения с БД хранилища данных
logger.info('Loading data to DWH...')
dwh_db_conn = create_engine(DWH_DB_URI)




#Extract yesterday's updated rows from source db
query = """SELECT plate_num,
                  update_dt AS start_dt,
                  model AS model_name,
                  revision_dt,
                  '9999-01-01 00:00:00' AS end_dt
          FROM main.car_pool
          WHERE CAST(update_dt AS DATE) = date('now') - 1"""
updated_cars = pd.read_sql(query, source_db_conn)
updated_cars.plate_num = updated_cars.plate_num.str.strip()

#Extract car table from DWH
query = 'SELECT * FROM dwh.dwh_kazan.dim_cars'
dwh_cars = pd.read_sql(query, dwh_db_conn)

#Get plate nums of cars we must update in DWH
plates_to_be_updated = updated_cars.plate_num.values

#Update end_dt values in DWH
mask = (dwh_cars['plate_num'].isin(plates_to_be_updated)) & (dwh_cars['end_dt'] == '9999-01-01 00:00:00')
dct = dict(updated_cars[['plate_num','start_dt']].values)
dwh_cars.loc[mask, 'end_dt'] = dwh_cars.loc[mask, :].apply((lambda x: x.end_dt.replace(x.end_dt, str(dct[x.plate_num]))), axis=1)

#Append new rows to DWH table
updated_dwh_cars = dwh_cars.append(updated_cars)
updated_dwh_cars.reset_index(drop=True, inplace=True)


#Extract yesterday's updated rows from source db
query = """ SELECT 
              md5(last_name  first_name  middle_name || birth_dt) AS personnel_num,
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
            WHERE CAST(update_dt AS DATE) = date('now') - 1"""
updated_drivers = pd.read_sql(query, source_db_conn)

#Extract drivers table from DWH
query = "SELECT * FROM dwh.dwh_kazan.dim_drivers"
dwh_drivers = pd.read_sql(query, dwh_db_conn)

#Get personnel_num of drivers we must update in DWH
personnels_to_be_updated = updated_drivers.personnel_num.values

#Update end_dt values in DWH
mask = (dwh_drivers['personnel_num'].isin(personnels_to_be_updated)) & (dwh_drivers['end_dt'] == '9999-01-01 00:00:00')
dct = dict(updated_drivers[['personnel_num','start_dt']].values)
dwh_drivers.loc[mask, 'end_dt'] = dwh_drivers.loc[mask, :].apply((lambda x: x.end_dt.replace(x.end_dt, str(dct[x.personnel_num]))), axis=1)

#Append new rows to DWH table
updated_dwh_drivers = dwh_drivers.append(updated_drivers)
updated_dwh_drivers.reset_index(drop=True, inplace=True)


#Extract yesterday's updated clients from source db
query = """ SELECT 
              client_phone AS phone_num,
              dt AS start_dt,
              card_num,
              LEAD (dt, 1, '9999-01-01 00:00:00') OVER (PARTITION BY client_phone ORDER BY dt) AS end_dt
            FROM main.rides
            WHERE CAST(dt AS DATE) = date('now') - 1"""
updated_clients = pd.read_sql(query, source_db_conn)

#Extract clients table from DWH
query = "SELECT * FROM dwh.dwh_kazan.dim_clients"
dwh_clients = pd.read_sql(query, dwh_db_conn)

#Get phone_num of clients we must update in DWH
phones_to_be_updated = updated_clients.phone_num.values

#Update end_dt values in DWH
mask = (dwh_clients['phone_num'].isin(phones_to_be_updated)) & (dwh_clients['end_dt'] == '9999-01-01 00:00:00')
dct = dict(updated_clients.groupby(updated_clients.phone_num).min().reset_index()[['phone_num','start_dt']].values)
dwh_clients.loc[mask, 'end_dt'] = dwh_clients.loc[mask, :].apply((lambda x: x.end_dt.replace(x.end_dt, str(dct[x.phone_num]))), axis=1)

#Append new rows to DWH table
updated_dwh_clients = dwh_clients.append(updated_clients)
updated_dwh_clients.reset_index(drop=True, inplace=True)


dwh_db_conn.execute('TRUNCATE dim_cars, dim_clients, dim_drivers;')

updated_dwh_clients.to_sql('dim_clients', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
updated_dwh_cars.to_sql('dim_cars', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
updated_dwh_drivers.to_sql('dim_drivers', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

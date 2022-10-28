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
# Initial data load - Будем загружать все доступные данные
# --------------------------------------------------------



# EXTRACT

# Скачивание всех файлов из директории FTP сервера
def get_all_items(dir: str):
    # Соединяемся по протоколу TLS
    ftps = FTP_TLS(host=SOURCE_FTP_HOST, user=SOURCE_FTP_USER, passwd=SOURCE_FTP_PWD)
    ftps.prot_p()
    ftps.cwd('../' + dir)
    items = ftps.nlst()
    for item in items:
        with open(os.path.join(dir + '/', item), 'wb') as file:
            ftps.retrbinary('RETR ' + item, file.write)
    ftps.quit()

# Качаем путевые листы и платежи
logger.info('Downloading files from FTP...')
get_all_items('waybills')
get_all_items('payments')

# Собираем датафрейм из всех путевых листов
waybills = pd.DataFrame()
waybills_dir = os.getcwd() + '/waybills'
for file in sorted(os.listdir(waybills_dir)):
    if file.endswith('.xml'):
        # Применяем XML stylesheet для корректного чтения
        waybills = pd.concat([waybills, (pd.read_xml('waybills/' + file, stylesheet='waybill.xsl'))], ignore_index=True)
    else:
        continue
# Переводим строки в datetime
waybills['issuedt'] = pd.to_datetime(waybills['issuedt'])
waybills['start'] = pd.to_datetime(waybills['start'])
waybills['stop'] = pd.to_datetime(waybills['stop'])

# Собираем датафрейм из всех платежей
payments = pd.DataFrame()
payments_dir = os.getcwd() + '/payments'
for file in sorted(os.listdir(payments_dir)):
    if file.endswith('.csv'):
        payment = pd.read_csv('payments/' + file, sep='\t', names = ['transaction_dt', 'card_num', 'transaction_amt'])
        payments = pd.concat([payments, payment], ignore_index=True)
    else:
        continue
# Переводим строки в datetime  
payments['transaction_dt'] = pd.to_datetime(payments['transaction_dt'], dayfirst=True)

# Создаем движок для соединения с БД источником
logger.info('Querying data from source DB...')
source_db_conn = create_engine(SOURCE_DB_URI, connect_args={'sslmode':'require'})

# Забираем все поездки
rides = pd.read_sql('SELECT * FROM main.rides', source_db_conn)

# Забираем все статусы машин
movement = pd.read_sql('SELECT * FROM main.movement', source_db_conn, index_col='movement_id')
movement['car_plate_num'] = movement['car_plate_num'].str.strip() # remove whitespaces

# Забираем всех водителей
drivers = pd.read_sql(
    '''
    SELECT md5(last_name || first_name || middle_name || birth_dt) AS personnel_num, * 
    FROM main.drivers
    ''',
    source_db_conn
)



# TRANSFORM

logger.info('Transforming fact tables...')

# Переводим информацию о времени статусов для каждой поездки в сводную таблицу
rides_time = movement.pivot(index='ride', columns='event', values='dt')
rides_time['END'] = rides_time['END'].fillna(rides_time['CANCEL'])
rides_time = rides_time.reindex(columns=['READY', 'BEGIN', 'END'])
rides_time = rides_time.rename(columns={'READY':'ride_arrival_dt', 'BEGIN':'ride_start_dt', 'END':'ride_end_dt'})

# Фильтруем статусы для того чтобы узнать завершенные поездки
rides_finish = movement.query("event == 'END' or event == 'CANCEL'")

# Получаем информацию по завершенным поездкам (кроме номера водителя)
fact_rides = rides_finish.merge(rides_time, how='left', on='ride')\
    .merge(rides, how='left', left_on='ride', right_on='ride_id', suffixes=['_event', '_begin'])

# Подтягиваем информацию о водителях в путевые листы
fact_waybills = waybills.merge(drivers[['personnel_num', 'driver_license']], how='left', left_on='license', right_on='driver_license')
fact_waybills = fact_waybills.dropna()[['number', 'personnel_num', 'car', 'start', 'stop', 'issuedt']]
fact_waybills = fact_waybills.rename(columns={
    'number':'waybill_num',
    'personnel_num':'driver_pers_num',
    'car':'car_plate_num',
    'start':'work_start_dt',
    'stop':'work_end_dt',
    'issuedt':'issue_dt'
})

# Ищем нужного водителя через путевые листы по номеру авто и времени. Добавляем в фактические поездки.
# Наверное можно сделать как-то более элегантно, но я не придумал как :)
fact_rides['driver_pers_num'] = ''
for idx in fact_rides.index:
    fact_rides['driver_pers_num'][idx] = fact_waybills[
        (fact_rides['car_plate_num'][idx] == fact_waybills.car_plate_num) & 
        (fact_rides['dt_begin'][idx] > fact_waybills.work_start_dt) & 
        (fact_rides['dt_begin'][idx] < fact_waybills.work_end_dt)
    ].driver_pers_num.values[0]
fact_rides = fact_rides[['ride', 'point_from', 'point_to', 'distance', 'price', 'client_phone', 'driver_pers_num', 'car_plate_num', 'ride_arrival_dt', 'ride_start_dt', 'ride_end_dt']]
fact_rides = fact_rides.rename(columns={
    'ride':'ride_id',
    'point_from':'point_from_txt',
    'point_to':'point_to_txt',
    'distance':'distance_val',
    'price':'price_amt',
    'client_phone':'client_phone_num'
})

logger.info('Transforming dimension tables...')

# Делаем измерение клиентов через оконную функцию
dim_clients = pd.read_sql(
    '''
    SELECT 
        client_phone AS phone_num,
        dt AS start_dt,
        card_num,
        LEAD (dt, 1, '9999-01-01 00:00:01') OVER (PARTITION BY client_phone ORDER BY dt) - INTERVAL '1 second' AS end_dt
    FROM
    (
        SELECT 
            client_phone,
            card_num,
            MIN(dt) AS dt
        FROM main.rides
        GROUP BY client_phone, card_num
    ) AS cards
    ''', 
    source_db_conn
)

# Делаем измерение автомобилей
dim_cars = pd.read_sql(
    '''
    SELECT 
        plate_num,
        update_dt AS start_dt,
        model AS model_name,
        revision_dt,
        '9999-01-01 00:00:00' AS end_dt
    FROM main.car_pool
    ''', 
    source_db_conn
)

# Делаем измерение водителей
dim_drivers = pd.read_sql(
    '''
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
    ''', 
    source_db_conn
)



# LOAD

# Создаем движок для соединения с БД хранилища данных
logger.info('Loading data to DWH...')
dwh_db_conn = create_engine(DWH_DB_URI, connect_args={'sslmode':'require'})

# Сбрасываем все строки в хранилище
dwh_db_conn.execute('TRUNCATE dim_cars, dim_clients, dim_drivers, fact_payments, fact_rides, fact_waybills RESTART IDENTITY;')

# Загружаем данные в соответствующие таблицы
payments.to_sql('fact_payments', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
fact_waybills.to_sql('fact_waybills', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
fact_rides.to_sql('fact_rides', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
dim_clients.to_sql('dim_clients', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
dim_cars.to_sql('dim_cars', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')
dim_drivers.to_sql('dim_drivers', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

# Удаление всех файлов из директории с диска
def delete_all_files(dir: str):
    path = os.getcwd() + '/' + dir
    filelist = [ f for f in os.listdir(path)]
    for f in filelist:
        os.remove(os.path.join(path, f))

# Удаляем все скачанные путевые листы и платежи с диска
logger.info('Cleaning downloadled files from disk...')
delete_all_files('waybills')
delete_all_files('payments')

logger.success("Script executed succesfully")
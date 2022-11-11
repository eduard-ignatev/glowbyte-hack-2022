import os
import pandas as pd
import hashlib
import datetime
import pangres

from dotenv import load_dotenv
from ftplib import FTP_TLS
from dateutil import parser
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from loguru import logger

# Загружаем credentials из переменных окружения
load_dotenv()
SOURCE_DB_URI = os.environ['SOURCE_DB_URI']
SOURCE_FTP_HOST = os.environ['SOURCE_FTP_HOST']
SOURCE_FTP_USER = os.environ['SOURCE_FTP_USER']
SOURCE_FTP_PWD = os.environ['SOURCE_FTP_PWD']
DWH_DB_URI = os.environ['DWH_DB_URI']

# Время запуска скрипта
etl_start_dt = datetime.datetime.now()

# -----------------------------------------------------------------------
# Incremental data load - Загружаем данные с последнего успешного запуска
# -----------------------------------------------------------------------



try:
    
    ### EXTRACT

    # Создаем движок для соединения с БД источником
    source_db_conn = create_engine(SOURCE_DB_URI, connect_args={'sslmode': 'require'})

    # Создаем движок для соединения с БД хранилища данных
    dwh_db_conn = create_engine(DWH_DB_URI, connect_args={'sslmode': 'require'})

    # Получаем время последнего успешного запуска
    last_etl_dt = dwh_db_conn.execute(
        '''
        SELECT COALESCE(MAX(bd.loaded_until), '1900-01-01 00:00:00')
        FROM dwh_kazan.work_batchdate AS bd
        WHERE bd.status = 'Success'
        '''
    ).fetchone()[0]

    # Путевые листы забираем с запасом +12 часов, поездки с запасом +2 часа
    waybills_extract_dt = last_etl_dt - datetime.timedelta(hours=12)
    rides_extract_dt =  last_etl_dt - datetime.timedelta(hours=2)

    # Скачивание файлов из директории FTP сервера с указанного времени
    def get_delta_items(dir: str, dt: datetime.datetime):
        # Соединяемся по протоколу TLS
        ftps = FTP_TLS(host=SOURCE_FTP_HOST, user=SOURCE_FTP_USER, passwd=SOURCE_FTP_PWD)
        ftps.prot_p()
        ftps.cwd('../' + dir)
        items_to_extract = []
        items = []
        ftps.retrlines('LIST', items.append)
        for item in items:
            tokens = item.split(maxsplit = 9)
            name = tokens[8]
            timestamp_str = tokens[5] + " " + tokens[6] + " " + tokens[7]
            timestamp = parser.parse(timestamp_str)
            if timestamp > dt:
                items_to_extract.append(name)
        for item in items_to_extract:
            with open(os.path.join(os.path.dirname(__file__), dir, item), 'wb') as file:
                ftps.retrbinary('RETR ' + item, file.write)
        ftps.quit()

    # Качаем новые путевые листы и платежи
    logger.info('Downloading files from FTP...')
    get_delta_items('waybills', waybills_extract_dt)
    get_delta_items('payments', last_etl_dt)

    # Собираем датафрейм из новых путевых листов
    waybills = pd.DataFrame()
    waybills_dir = os.path.join(os.path.dirname(__file__), 'waybills')
    xsl_file = xsl_file = os.path.join(os.path.dirname(__file__), 'waybill.xsl')
    for file in sorted(os.listdir(waybills_dir)):
        if file.endswith('.xml'):
            waybills = pd.concat([waybills, (pd.read_xml(os.path.join(waybills_dir, file), stylesheet=xsl_file))], ignore_index=True)
        else:
            continue
    # Переводим строки в datetime
    waybills['issuedt'] = pd.to_datetime(waybills['issuedt'])
    waybills['start'] = pd.to_datetime(waybills['start'])
    waybills['stop'] = pd.to_datetime(waybills['stop'])

    # Собираем датафрейм из новых платежей
    payments = pd.DataFrame()
    payments_dir = os.path.join(os.path.dirname(__file__), 'payments')
    for file in sorted(os.listdir(payments_dir)):
        if file.endswith('.csv'):
            payment = pd.read_csv(os.path.join(payments_dir, file), sep='\t', names = ['transaction_dt', 'card_num', 'transaction_amt'])
            payments = pd.concat([payments, payment], ignore_index=True)
        else:
            continue
    # Делаем уникальный id транзакции с помощью md5 хэша
    payments['transaction_id'] = payments['transaction_dt'].astype(str) + payments['card_num'].astype(str)
    payments['transaction_id'] = payments['transaction_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    # Переводим строки в datetime  
    payments['transaction_dt'] = pd.to_datetime(payments['transaction_dt'], dayfirst=True)
    payments = payments.set_index('transaction_id')

    logger.info('Querying data from source DB...')

    # Забираем новые поездки
    rides = pd.read_sql(
        'SELECT * FROM main.rides WHERE dt > %(dt)s', 
        source_db_conn,
        params={'dt': rides_extract_dt}
    )

    # Забираем новые статусы машин
    movement = pd.read_sql(
        'SELECT * FROM main.movement WHERE dt > %(dt)s', 
        source_db_conn,
        params={'dt': rides_extract_dt},
        index_col='movement_id'
    )
    movement['car_plate_num'] = movement['car_plate_num'].str.strip()

    # Забираем ВСЕХ водителей, тк искать нужно по всем
    drivers = pd.read_sql(
        'SELECT md5(last_name || first_name || middle_name || birth_dt) AS personnel_num, * FROM main.drivers',
        source_db_conn
    )



    ### TRANSFORM-LOAD (FACT)

    logger.info('Updating fact tables...')

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
    # Убираем поездки по которым попала только частичная информация
    fact_rides = fact_rides[fact_rides.client_phone.notna()]

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
    fact_waybills = fact_waybills.set_index('waybill_num')

    # Ищем нужного водителя через путевые листы по номеру авто и времени. Добавляем в фактические поездки.
    # Наверное можно сделать как-то более элегантно, но я не придумал как :)
    fact_rides['driver_pers_num'] = ''
    for idx in fact_rides.index:
        fact_rides.loc[idx, 'driver_pers_num'] = fact_waybills[
            (fact_rides.loc[idx, 'car_plate_num'] == fact_waybills['car_plate_num']) & 
            (fact_rides.loc[idx, 'dt_begin'] >= fact_waybills['work_start_dt']) & 
            (fact_rides.loc[idx, 'dt_begin'] <= fact_waybills['work_end_dt'])
        ]['driver_pers_num'].values[0]
    fact_rides = fact_rides[[
        'ride', 'point_from', 'point_to', 'distance', 'price', 'client_phone', 'driver_pers_num', 
        'car_plate_num', 'ride_arrival_dt', 'ride_start_dt', 'ride_end_dt']]
    fact_rides = fact_rides.rename(columns={
        'ride':'ride_id',
        'point_from':'point_from_txt',
        'point_to':'point_to_txt',
        'distance':'distance_val',
        'price':'price_amt',
        'client_phone':'client_phone_num'
    })
    fact_rides = fact_rides.set_index('ride_id')

    # Загружаем новые данные в фактовые таблицы методом UPSERT
    pangres.upsert(
        con=dwh_db_conn, 
        df=fact_rides,
        table_name='fact_rides',
        if_row_exists='ignore',
        schema='dwh_kazan'
    )
    pangres.upsert(
        con=dwh_db_conn, 
        df=fact_waybills,
        table_name='fact_waybills',
        if_row_exists='ignore',
        schema='dwh_kazan'
    )
    pangres.upsert(
        con=dwh_db_conn, 
        df=payments,
        table_name='fact_payments',
        if_row_exists='ignore',
        schema='dwh_kazan'
    )



    ### TRANSFORM-LOAD (DIM)

    logger.info('Updating dimension tables...')

    ### dim_cars
    # Get car_pool updates
    updated_cars = pd.read_sql(
        '''
        SELECT plate_num,
            update_dt AS start_dt,
            model AS model_name,
            revision_dt,
            '9999-01-01 00:00:00' AS end_dt
        FROM main.car_pool
        WHERE update_dt > %(dt)s
        ''', 
        source_db_conn,
        params={'dt': last_etl_dt}
    )
    updated_cars.plate_num = updated_cars.plate_num.str.strip()

    # Upload car_pool updates to temp_table
    updated_cars.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

    # Update end_dt for previously loaded rows
    dwh_db_conn.execute(
        '''
        UPDATE dim_cars AS f
        SET f.end_dt = t.start_dt - INTERVAL '1 second'
        FROM work_temp_table AS t
        WHERE f.plate_num = t.plate_num
        AND f.end_dt = '9999-01-01 00:00:00'
        AND t.revision_dt != f.revision_dt
        '''
    )

    # Upload car_pool updates to target table
    updated_cars.to_sql('dim_cars', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

    ### dim_drivers
    # Get drivers updates
    updated_drivers = pd.read_sql(
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
        WHERE update_dt > %(dt)s
        ''', 
        source_db_conn,
        params={'dt': last_etl_dt}
    )

    # Upload drivers updates to temp_table
    updated_drivers.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

    # Update end_dt for previously loaded rows
    dwh_db_conn.execute(
        '''
        UPDATE dim_drivers AS f
        SET f.end_dt = t.start_dt - INTERVAL '1 second'
        FROM work_temp_table AS t
        WHERE f.personnel_num = t.personnel_num
        AND f.end_dt = '9999-01-01 00:00:00'
        AND t.card_num != f.card_num
        '''
    )

    # Upload drivers updates to target table
    updated_drivers.to_sql('dim_drivers', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

    ### dim_clients
    # Get clients updates
    updated_clients = pd.read_sql(
        '''
        SELECT
            client_phone AS phone_num,
            dt AS start_dt,
            card_num,
            LEAD(dt, 1, '9999-01-01 00:00:01') OVER(PARTITION BY client_phone ORDER BY dt) - INTERVAL '1 second' AS end_dt
        FROM
        (
            SELECT 
                client_phone,
                card_num,
                MIN(dt) AS dt
            FROM main.rides
            GROUP BY client_phone, card_num
        ) AS cards
        WHERE dt > %(dt)s
        ''',
        source_db_conn,
        params={'dt': last_etl_dt}
    )

    # Upload drivers updates to temp_table
    updated_clients.to_sql('work_temp_table', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='replace')

    # Update end_dt for previously loaded rows
    dwh_db_conn.execute(
        '''
        UPDATE dim_clients AS f
        SET f.end_dt = t.start_dt - INTERVAL '1 second'
        FROM work_temp_table AS t
        WHERE f.phone_num = t.phone_num
        AND f.end_dt = '9999-01-01 00:00:00'
        AND t.card_num != f.card_num
        '''
    )

    # Upload clients updates to work_dim_clients table
    updated_clients.to_sql('dim_clients', con=dwh_db_conn, schema='dwh_kazan', index=False, if_exists='append')

    # Deduplicate dim_clients after adding new rows
    dwh_db_conn.execute(
        '''
        DELETE
        FROM dim_clients dc1
        USING dim_clients dc2
        WHERE dc1.ctid < dc2.ctid
        AND dc1.phone_num = dc2.phone_num
        AND dc1.start_dt = dc2.start_dt;
        '''
    )

    # Время завершения и выполнения скрипта
    etl_end_dt = datetime.datetime.now()
    etl_duration = etl_end_dt - etl_start_dt

    # Логгируем успешное выполнение в рабочую таблицу хранилища
    dwh_db_conn.execute(
        text("INSERT INTO dwh_kazan.work_batchdate (loaded_until, status) VALUES(:dt, :st)"),
        {'dt': etl_start_dt, 'st': 'Success'}
    )

    logger.success("Script executed succesfully in {} seconds", etl_duration.total_seconds())



except Exception:

    # Логгируем неудачное выполнение в рабочую таблицу хранилища
    dwh_db_conn.execute(
        text("INSERT INTO dwh_kazan.work_batchdate (loaded_until, status) VALUES(:dt, :st)"),
        {'dt': etl_start_dt, 'st': 'Failure'}
    )

    logger.exception("Script executed with unexpected error")



finally:

    # Удаление всех файлов из директории с диска
    def delete_all_files(dir: str):
        path = os.path.join(os.path.dirname(__file__), dir)
        filelist = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and not f.endswith('.gitkeep')]
        for f in filelist:
            os.remove(os.path.join(path, f))

    # Удаляем все скачанные путевые листы и платежи с диска
    logger.info('Cleaning downloadled files from disk...')
    delete_all_files('waybills')
    delete_all_files('payments')

    # Дропаем временные таблицы в хранилище
    dwh_db_conn.execute('DROP TABLE IF EXISTS work_temp_table')
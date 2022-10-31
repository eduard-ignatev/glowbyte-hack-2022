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
# Data Marts - Обновление витрин данных для отчетности
# -----------------------------------------------------------------------



try:

    # Создаем движок для соединения с БД хранилища данных
    dwh_db_conn = create_engine(DWH_DB_URI, connect_args={'sslmode': 'require'})

    ### 1. Выплата водителям
    # Считаем информацию по выплатам из fact_rides
    # Джойним с информацией о водителях из dim_drivers
    dwh_db_conn.execute(
        '''
        INSERT INTO rep_drivers_payments
        SELECT dd.personnel_num, dd.last_name, dd.first_name, dd.middle_name, dd.card_num, fr.amount, fr.report_dt
        FROM
        (
            SELECT 
                driver_pers_num,
                SUM(distance_val) AS total_distance, 
                SUM(price_amt) AS total_cash,
                ROUND(SUM(price_amt) * 0.8 - 47.26 * 7 * SUM(distance_val) / 100 - 5 * SUM(distance_val), 2) AS amount,
                ride_arrival_dt::date AS report_dt
            FROM fact_rides
            WHERE ride_arrival_dt::date = current_date - INTEGER '1'
            GROUP BY driver_pers_num, ride_arrival_dt::date
        ) fr
        JOIN 
        (
            SELECT 
                personnel_num,
                last_name,
                first_name,
                middle_name,
                card_num,
                current_date - INTEGER '1' AS report_dt
            FROM dim_drivers
            WHERE current_date - INTEGER '1' BETWEEN start_dt AND end_dt
        ) dd
        ON fr.driver_pers_num = dd.personnel_num;
        '''
    )



    ### 4. “Знай своего клиента”
    # Строим на основе dim_clients
    # Подтягиваем информацию по поступившим платежам по каждой карте
    # Подтягиваем информацию по поездкам и стоимости услуг для каждого номера и карты
    dwh_db_conn.execute(
        '''
        INSERT INTO rep_clients_hist 
        SELECT 
            md5(dc.phone_num || dc.start_dt) AS client_id,
            dc.phone_num,
            frg.rides_cnt,
            frg.cancelled_cnt,
            frg.spent_amt,
            frg.spent_amt - fp.total_paid AS debt_amt,
            dc.start_dt,
            dc.end_dt,
            dc.deleted_flag
        FROM dim_clients dc
        JOIN
        (
            SELECT
                SUBSTRING(card_num, 1, 4) || ' ' || SUBSTRING(card_num, 5, 4) || ' ' || SUBSTRING(card_num, 9, 4) || ' ' || SUBSTRING(card_num, 13, 4) AS card_num,
                SUM(transaction_amt) AS total_paid
            FROM fact_payments
            GROUP BY card_num
        ) fp
        ON dc.card_num = fp.card_num
        JOIN
        (
            SELECT 
                fr.client_phone_num,
                dc.card_num,
                COUNT(fr.ride_id) AS rides_cnt,
                COUNT(CASE WHEN fr.ride_start_dt IS NULL THEN 1 ELSE NULL END) AS cancelled_cnt,
                SUM(CASE WHEN fr.ride_start_dt IS NULL THEN NULL ELSE fr.price_amt END) AS spent_amt
            FROM fact_rides fr
            JOIN dim_clients dc 
            ON fr.client_phone_num = dc.phone_num AND (fr.ride_arrival_dt BETWEEN dc.start_dt AND dc.end_dt)
            GROUP BY fr.client_phone_num, dc.card_num
        ) frg
        ON dc.phone_num = frg.client_phone_num AND dc.card_num = frg.card_num;
        '''
    )











    # Update rep_drivers_overtime data mart
    query = """
        INSERT INTO dwh_kazan.rep_drivers_overtime(
            personnel_num,
            total_work_time,
            period_start
        )
        SELECT driver_pers_num AS personnel_num,
            CAST(AVG(total_work_time) AS TIME) AS total_work_time,
            MAX(work_end_dt) - INTERVAL '24 hour' AS period_start
        FROM (
            SELECT 
            driver_pers_num,
            work_start_dt,
            work_end_dt,
            CAST(work_end_dt - work_start_dt AS TIME) AS work_time,
            CAST(SUM(work_end_dt - work_start_dt) OVER (PARTITION BY driver_pers_num) AS TIME) AS total_work_time,
            MAX(work_end_dt) OVER (PARTITION BY driver_pers_num) - MIN(work_start_dt) OVER (PARTITION BY driver_pers_num) AS period
            FROM dwh_kazan.fact_waybills
            WHERE work_end_dt > %(dt)s
        ) AS query
        WHERE total_work_time > INTERVAL '8 hour' AND period < INTERVAL '24 hour'
        GROUP BY driver_pers_num
    """
    db_conn.execute(query)

except Exception:
    pass
import os
import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Загружаем credentials из переменных окружения
load_dotenv()
SOURCE_DB_URI = os.environ['SOURCE_DB_URI']
SOURCE_FTP_HOST = os.environ['SOURCE_FTP_HOST']
SOURCE_FTP_USER = os.environ['SOURCE_FTP_USER']
SOURCE_FTP_PWD = os.environ['SOURCE_FTP_PWD']
DWH_DB_URI = os.environ['DWH_DB_URI']

# Время запуска скрипта
update_start_dt = datetime.datetime.now()

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
    # В итоге делаем UPSERT и обновляем измененные записи 
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
        ON dc.phone_num = frg.client_phone_num AND dc.card_num = frg.card_num
        ON CONFLICT ON CONSTRAINT rep_clients_hist_pk DO UPDATE
        SET 
            rides_cnt = EXCLUDED.rides_cnt, 
            cancelled_cnt = EXCLUDED.cancelled_cnt, 
            spent_amt = EXCLUDED.spent_amt, 
            debt_amt = EXCLUDED.debt_amt,
            end_dt = EXCLUDED.end_dt,
            deleted_flag = EXCLUDED.deleted_flag;
        '''
    )


    ### 2. Водители-нарушители
    # Update rep_drivers_violations data mart
    query = """
        INSERT INTO dwh_kazan.rep_drivers_violations(
            personnel_num,
            ride,
            speed,
            violations_cnt
        )
        SELECT q1.personnel_num, q1.ride, q1.speed, q1.violations_cnt + q2.violations_cnt AS violations_cnt
        FROM 
        (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY personnel_num ORDER BY ride) AS violations_cnt
            FROM (
            SELECT driver_pers_num AS personnel_num,
                ride_id AS ride,
                ROUND(distance_val / (EXTRACT(EPOCH FROM ride_end_dt - ride_start_dt) / 3600), 2)  AS speed
            FROM dwh_kazan.fact_rides
            WHERE ride_start_dt IS NOT NULL AND ride_end_dt > %(dt)s
            ) AS subquery
        WHERE speed > 85
        ) AS q1
        INNER JOIN
        (
        SELECT
            personnel_num,
            MAX(violations_cnt) AS violations_cnt
            FROM dwh_kazan.rep_drivers_violations
            GROUP BY personnel_num
        ) AS q2
        ON q1.personnel_num = q2.personnel_num
    """
    dwh_db_conn.execute(query)


    ### 3. Перерабатывающие водители
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
    dwh_db_conn.execute(query)

    # Время завершения и выполнения скрипта
    update_end_dt = datetime.datetime.now()
    update_duration = update_end_dt - update_start_dt

    # Логгируем успешное выполнение в рабочую таблицу хранилища
    #

    logger.success("Script executed succesfully in {} seconds", etl_duration.total_seconds())

except Exception:
    
    # Логгируем неудачное выполнение в рабочую таблицу хранилища
    #

    logger.exception("Script executed with unexpected error")
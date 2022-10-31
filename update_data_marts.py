
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





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
db_conn.execute(query)

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

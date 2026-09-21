-- Gold: one row per recording session, with the sensor aggregates a fleet
-- engineer would actually want.
--
-- The grain is (session_id, robot_id) — one row per robot run. That is the unit
-- anyone asks questions about: "did this run look healthy?", "which robot is
-- drifting?". Aggregating to anything coarser would lose the ability to spot a
-- single bad run, which is the entire point of a sensor-health table.
--
-- Rates are computed from actual sample counts and elapsed time rather than
-- from the nominal publish rate, so a session where the IMU silently dropped
-- 35% of its messages cannot report a healthy 100 Hz. Deriving the rate from
-- the data is what makes this table able to detect the failure it is meant to.

{{ config(materialized='table') }}

with readings as (

    select * from {{ ref('silver_sensor_readings') }}

),

-- Per-session, per-topic shape. Grouping by topic first and pivoting after
-- keeps each sensor's columns together and avoids a wall of conditional
-- aggregates that is impossible to read.
by_topic as (

    select
        session_id,
        robot_id,
        topic,
        count(*)                    as sample_count,
        min(t_ns)                   as first_t_ns,
        max(t_ns)                   as last_t_ns,
        max(t_offset_s)             as duration_s,
        count(*) / nullif(max(t_offset_s), 0) as sample_rate_hz,

        -- IMU / odometry
        avg(lin_acc_mag)            as mean_lin_acc_mag,
        max(lin_acc_mag)            as max_lin_acc_mag,
        avg(ang_vel_z)              as mean_ang_vel_z,
        stddev_samp(ang_vel_z)      as stddev_ang_vel_z,
        avg(lin_vel_x)              as mean_lin_vel_x,
        max(lin_vel_x)              as max_lin_vel_x,

        -- Pose spread: how far the robot actually travelled. max - min rather
        -- than a sum of deltas, because we want the extent of the path, and
        -- summing step deltas would accumulate sensor noise into a fake
        -- odometer.
        max(pos_x) - min(pos_x)     as path_extent_x,
        max(pos_y) - min(pos_y)     as path_extent_y,

        -- LiDAR
        avg(mean_range)             as mean_scan_range,
        min(min_range)              as closest_obstacle_m,
        avg(beam_return_ratio)      as mean_beam_return_ratio

    from readings
    group by session_id, robot_id, topic

),

-- The health verdict. Deliberately explicit thresholds rather than a score:
-- a score hides which check failed, and this table's job is to name the
-- failure. The nominal IMU rate is 100 Hz; a session below 90% of that has
-- lost enough samples to matter.
session_health as (

    select
        session_id,
        robot_id,
        max(case when topic = '/imu/data' then sample_rate_hz end) as imu_rate_hz,
        max(case when topic = '/odom' then sample_rate_hz end)     as odom_rate_hz,
        max(case when topic = '/scan' then sample_rate_hz end)     as scan_rate_hz,
        max(duration_s)                                            as duration_s,
        sum(sample_count)                                          as total_samples,
        max(case when topic = '/imu/data' then mean_lin_acc_mag end) as imu_mean_lin_acc_mag,
        max(case when topic = '/imu/data' then stddev_ang_vel_z end) as imu_stddev_ang_vel_z,
        max(case when topic = '/imu/data' then mean_ang_vel_z end)   as imu_mean_ang_vel_z,
        max(case when topic = '/odom' then max_lin_vel_x end)        as max_linear_vel_mps,
        max(case when topic = '/odom' then path_extent_x end)        as path_extent_x,
        max(case when topic = '/odom' then path_extent_y end)        as path_extent_y,
        max(case when topic = '/scan' then mean_scan_range end)      as mean_scan_range_m,
        max(case when topic = '/scan' then closest_obstacle_m end)   as closest_obstacle_m,
        max(case when topic = '/scan' then sample_count end)         as scan_sample_count
    from by_topic
    group by session_id, robot_id

),

final as (

    select
        h.session_id,
        h.robot_id,
        -- Duration and rates are rounded to 6 decimals. Not cosmetic: DuckDB
        -- aggregates in parallel across 4 threads, and floating-point addition
        -- is not associative, so an unrounded average can differ in the last
        -- one or two digits between two runs over identical input. Rounding at
        -- the gold boundary makes the output genuinely reproducible, which is
        -- what `make verify` asserts. 6 decimals is far more precision than any
        -- sensor measurement justifies (the data is ~3 decimals of real signal).
        round(h.duration_s, 6)           as duration_s,
        h.total_samples,
        round(h.imu_rate_hz, 6)          as imu_rate_hz,
        round(h.odom_rate_hz, 6)         as odom_rate_hz,
        round(h.scan_rate_hz, 6)         as scan_rate_hz,
        -- Nominal IMU rate is 100 Hz. Anything under 90% of nominal means the
        -- sensor dropped samples — a real failure mode, not a rounding wobble.
        case
            when h.imu_rate_hz is null then 'imu_missing'
            when h.imu_rate_hz < 90.0  then 'imu_degraded'
            when h.scan_sample_count = 0 then 'lidar_missing'
            else 'healthy'
        end                              as health_status,
        -- IMU rate as a percentage of the nominal 100 Hz, so the verdict above
        -- is explainable rather than a bare label. Derived here rather than in
        -- `session_health` because it is presentation, not aggregation.
        round(100.0 * h.imu_rate_hz / 100.0, 6) as imu_rate_pct_of_nominal,
        round(h.imu_mean_lin_acc_mag, 6) as imu_mean_lin_acc_mag,
        round(h.imu_stddev_ang_vel_z, 6) as imu_stddev_ang_vel_z,
        round(h.imu_mean_ang_vel_z, 6)   as imu_mean_ang_vel_z,
        round(h.max_linear_vel_mps, 6)   as max_linear_vel_mps,
        round(h.path_extent_x, 6)        as path_extent_x,
        round(h.path_extent_y, 6)        as path_extent_y,
        round(h.mean_scan_range_m, 6)    as mean_scan_range_m,
        round(h.closest_obstacle_m, 6)   as closest_obstacle_m
    from session_health as h

)

select * from final

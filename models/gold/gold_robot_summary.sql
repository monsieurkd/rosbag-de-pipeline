-- Gold: per-robot rollup across all of that robot's sessions.
--
-- Separate from `gold_session_health` because the questions are different.
-- Session health answers "was this run okay?"; this answers "is this robot
-- reliable?", which needs a count of bad runs rather than one row per run.
--
-- Kept as its own model instead of a window function over session health so
-- that the fleet-level view can be consumed on its own (and so the test on
-- `sessions_recorded` has something to assert against).

{{ config(materialized='table') }}

with sessions as (

    select * from {{ ref('gold_session_health') }}

),

per_robot as (

    select
        robot_id,
        count(*)                                        as sessions_recorded,
        count(*) filter (where health_status = 'healthy') as healthy_sessions,
        count(*) filter (where health_status != 'healthy') as flagged_sessions,
        sum(duration_s)                                 as total_recorded_s,
        sum(total_samples)                              as total_samples,
        avg(imu_rate_hz)                                as mean_imu_rate_hz,
        min(imu_rate_hz)                                as min_imu_rate_hz,
        avg(mean_scan_range_m)                          as mean_scan_range_m,
        min(closest_obstacle_m)                         as closest_obstacle_m,
        max(max_linear_vel_mps)                         as max_linear_vel_mps,
        max(path_extent_x)                              as max_path_extent_x,
        max(path_extent_y)                              as max_path_extent_y
    from sessions
    group by robot_id

)

select
    robot_id,
    sessions_recorded,
    healthy_sessions,
    flagged_sessions,
    -- Share of runs that came back clean.
    round(100.0 * healthy_sessions / nullif(sessions_recorded, 0), 2) as healthy_session_pct,
    -- Rounded here too, for the same reason as gold_session_health: these are
    -- averages over floats, and parallel aggregation is not associative, so an
    -- unrounded mean is not reproducible between runs.
    round(total_recorded_s, 6)   as total_recorded_s,
    total_samples,
    round(mean_imu_rate_hz, 6)   as mean_imu_rate_hz,
    round(min_imu_rate_hz, 6)    as min_imu_rate_hz,
    round(mean_scan_range_m, 6)  as mean_scan_range_m,
    round(closest_obstacle_m, 6) as closest_obstacle_m,
    round(max_linear_vel_mps, 6) as max_linear_vel_mps,
    round(max_path_extent_x, 6)  as max_path_extent_x,
    round(max_path_extent_y, 6)  as max_path_extent_y
from per_robot

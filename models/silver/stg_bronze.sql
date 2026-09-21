-- stg_bronze: the union of the three topic tables landed as Parquet.
--
-- This is the one place dbt touches the filesystem. Everything above it works
-- on relations, which keeps the transform layer portable: swapping the Parquet
-- source for S3, or DuckDB for a warehouse, is a change to this model alone.
--
-- The union is deliberately loose (`union all by name`) because the three
-- topics legitimately have different columns — IMU has no `pos_x`, LiDAR has no
-- `qx`. Filling them with nulls here is correct: a missing column means "this
-- sensor doesn't measure that", not "zero". Forcing a rigid schema would invent
-- readings that were never taken.

with

imu as (
    select * from read_parquet(
        'data/bronze/imu/**/*.parquet',
        hive_partitioning = true
    )
),

odom as (
    select * from read_parquet(
        'data/bronze/odom/**/*.parquet',
        hive_partitioning = true
    )
),

scan as (
    select * from read_parquet(
        'data/bronze/scan/**/*.parquet',
        hive_partitioning = true
    )
),

unioned as (
    select * from imu
    union all by name
    select * from odom
    union all by name
    select * from scan
)

select
    session_id,
    robot_id,
    topic,
    t_ns,
    t_s,
    qx, qy, qz, qw,
    ang_vel_x, ang_vel_y, ang_vel_z,
    lin_acc_x, lin_acc_y, lin_acc_z,
    pos_x, pos_y, pos_z,
    lin_vel_x,
    beam_count, valid_beams,
    min_range, mean_range, max_range

from unioned

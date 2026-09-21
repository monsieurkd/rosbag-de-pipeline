-- Silver: one clean, typed row per sensor message.
--
-- Bronze is deliberately raw: whatever Parquet holds, as it was written, with
-- no opinions. Silver is where the opinions live — types are pinned, units are
-- named, and the derived columns gold needs are computed once here rather than
-- repeatedly downstream.
--
-- Why a view instead of a table: silver must always reflect the latest bronze
-- landing. Materialising it would mean an `make ingest` that re-lands a single
-- session could leave a stale silver table behind — precisely the
-- non-idempotency this pipeline exists to avoid.

{{ config(materialized='view') }}

with source as (

    select * from {{ ref('stg_bronze') }}

),

enriched as (

    select
        session_id,
        robot_id,
        topic,

        -- Keep both the raw nanosecond stamp and a readable timestamp. Sensor
        -- pipelines need the integer for exact ordering and resampling (float
        -- seconds lose precision at nanosecond resolution and can reorder
        -- ties), but humans and BI tools need the timestamp.
        t_ns,
        make_timestamp(t_ns)                          as recorded_at,
        t_s,
        -- Elapsed time since the session's first sample. Computed once here
        -- because every rate and latency calculation downstream needs it, and
        -- recomputing a window function per model invites drift.
        t_s - min(t_s) over (partition by session_id) as t_offset_s,

        qx, qy, qz, qw,

        -- Heading from the quaternion, assuming planar motion (roll and pitch
        -- ~0 for a differential-drive robot on a floor). Derived here so gold
        -- can compare IMU heading against odometry heading without repeating
        -- the quaternion maths in two places.
        2.0 * atan2(qz, qw) as yaw_rad,

        ang_vel_x,
        ang_vel_y,
        ang_vel_z,
        lin_acc_x,
        lin_acc_y,
        lin_acc_z,

        -- Magnitude of linear acceleration. A healthy robot accelerating
        -- smoothly reads a stable value; the failing-IMU session spikes here,
        -- which is what the health model thresholds on.
        sqrt(lin_acc_x * lin_acc_x + lin_acc_y * lin_acc_y
             + lin_acc_z * lin_acc_z) as lin_acc_mag,

        pos_x,
        pos_y,
        pos_z,
        lin_vel_x,

        -- LiDAR columns. Null for IMU/odom rows: a topic that has no beams
        -- genuinely has no beam count. Null means "not measured", which is not
        -- the same as zero, and conflating the two would corrupt every average.
        beam_count,
        valid_beams,
        min_range,
        mean_range,
        max_range,

        -- Fraction of beams that returned a real distance rather than `inf`
        -- (ROS's convention for "hit nothing within range_max"). A LiDAR
        -- staring into open space reads a low ratio; this is the column that
        -- makes that visible instead of hiding it in a max.
        case
            when beam_count > 0
            then valid_beams / cast(beam_count as double)
        end as beam_return_ratio

    from source

)

select * from enriched

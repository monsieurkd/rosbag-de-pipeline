# ROS-Bag Telemetry Analytics Pipeline

An end-to-end data pipeline over **ROS 2 robot telemetry**: raw bag recordings of
IMU, odometry and LiDAR streams are landed as Parquet, then transformed through a
bronze → silver → gold medallion architecture into queryable analytics tables in
DuckDB, with data-quality tests at every layer and a mechanically-verified
idempotency guarantee.

Everything runs locally with no accounts, no credentials and no ROS install.

```bash
make setup    # venv + dependencies (~30s)
make build    # generate bags → land bronze → dbt silver/gold → test
make verify   # rebuild twice and PROVE the output is identical
```

---

## Why this project exists

Robot fleet data is genuinely awkward: sensor streams arrive at different rates
and in different shapes, some readings are *legitimately* full of infinities, and
a robot that silently drops 35% of its IMU samples looks perfectly normal unless
something is specifically measuring for it.

This pipeline is built around one question a fleet engineer actually asks:
**"is this robot reliable?"** The gold layer answers it by deriving sensor rates
*from the data* rather than trusting the nominal publish rate — so a session that
lost a third of its IMU messages cannot report a healthy 100 Hz.

## Architecture

```
data/raw/session_*/           raw ROS 2 bags (generated, gitignored)
        │
        │  ingest/land_bronze.py — read bags, land byte-faithful Parquet
        ▼
data/bronze/{imu,odom,scan}/session_id=<id>/*.parquet
        │
        │  dbt: models/silver/ — union, type, derive
        ▼
silver_sensor_readings        one typed row per sensor message (view)
        │
        │  dbt: models/gold/ — aggregate to the grain people ask about
        ▼
gold_session_health           one row per session + an explicit health verdict
gold_robot_summary            one row per robot — "is this robot reliable?"
```

| Layer | Materialised as | Why |
|---|---|---|
| Bronze | Parquet on disk | Raw and byte-faithful, so a transformation bug can never be mistaken for a lossy ingest |
| Silver | dbt **view** | Always reflects the latest landing — a materialised silver could go stale after a partial re-ingest |
| Gold | dbt **table** | The artifacts that get rebuilt and diffed |

## The data-quality layer

20 dbt tests run as part of `make build`: `unique`, `not_null`,
`accepted_values` and `relationships` (referential integrity between silver and
gold), plus 7 pytest tests over the ingest layer.

The health verdict in `gold_session_health` is deliberately an explicit label
rather than a score — a score hides which check failed, and the table's job is to
name the failure:

| `health_status` | Meaning |
|---|---|
| `healthy` | All sensors at expected rates |
| `imu_degraded` | IMU delivered under 90% of its nominal 100 Hz |
| `imu_missing` | No IMU samples at all in the session |
| `lidar_missing` | Session has LiDAR columns but zero scans |

## Idempotency — asserted, then proven

The pipeline guarantees that **reprocessing the same bags produces identical gold
tables**. That kind of claim rots quietly: it is easy to write in a README and
easy to break without noticing. So it is not a claim — it is a check.

`make verify` tears down the warehouse, rebuilds it from scratch into one DuckDB
file, rebuilds it *again* into a separate file, exports both gold layers, and
compares them byte-for-byte:

```
✓ gold_session_health identical
✓ gold_robot_summary identical

✓ idempotent: both rebuilds produced identical gold tables
```

This check earned its keep immediately. The first run **failed**, and the cause
was real: DuckDB aggregates floats in parallel across 4 threads, and
floating-point addition is not associative, so unrounded averages differed in the
last one or two digits between runs over byte-identical input. The fix was to
round at the gold boundary — documented in `models/gold/gold_session_health.sql`,
because the *reason* matters more than the `round()` call.

Bronze idempotency is enforced separately at ingest: landing a bag replaces its
partition rather than appending, so a re-run cannot silently double the rows.

## The robot data is synthetic — deliberately

Shipping multi-gigabyte recordings is not viable, and requiring a ROS install
would make the project un-runnable for anyone reviewing it. So
`generate/make_bags.py` synthesises recordings using
[`rosbags`](https://pypi.org/project/rosbags/) — pure Python, no ROS needed.

The output is a **genuine ROS 2 bag** that the official tooling can read, from a
seeded simulation, small enough to regenerate on demand (~5 MB/session).

The simulation is simple but self-consistent: odometry is derived from the
commanded velocity, IMU yaw tracks the same heading, and LiDAR ranges come from
ray-casting against the same room. That consistency is what lets the
data-quality tests assert something meaningful instead of re-testing noise.

Three sessions are generated, one per IMU archetype — `healthy`, `noisy`, and
`failing` (35% sample dropout). **A pipeline whose health checks only ever see
clean data has never been tested**, so the failing session is the point: it must
show up as `imu_degraded` in gold, and it does.

### Verified output

| session | robot | IMU Hz | status |
|---|---|---|---|
| session_001 | robot_alpha | 100.0 | healthy |
| session_002 | robot_alpha | 65.2 | **imu_degraded** |
| session_003 | robot_beta | 97.9 | healthy |

Derived purely from sample counts and elapsed time — the 65 Hz figure is the
pipeline detecting the injected dropout, not being told about it.

## Layout

```
generate/make_bags.py        seeded synthetic ROS 2 bag generator
ingest/land_bronze.py        bags → partitioned Parquet (idempotent)
models/silver/               union + typing + derivation
models/gold/                 session health, robot summary
models/schema.yml            data-quality tests + column docs
tools/verify_idempotency.py  the make verify comparison
tests/                       pytest over the ingest layer
```

## Design decisions worth flagging

- **Bronze is not a dbt layer.** If it were, reprocessing one bag would mean
  re-running the whole warehouse.
- **`union all by name`, not a rigid schema.** The three topics genuinely have
  different columns; a missing column means "this sensor doesn't measure that",
  which is not the same as zero. Forcing a schema would invent readings.
- **`inf` is kept, not nulled.** It is ROS's convention for "beam hit nothing
  within range_max" — a valid reading. It is excluded from `mean_range`
  (averaging infinity poisons every aggregate) but preserved in `max_range`.
- **Rates are measured, not assumed.** The whole health table depends on this.
- **Rounding at gold is deliberate.** See the idempotency section.

## Requirements

Python ≥ 3.11, [`uv`](https://github.com/astral-sh/uv), and `make`. Everything
else is installed by `make setup`. No ROS installation required.

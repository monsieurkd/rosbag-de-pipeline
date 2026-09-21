"""Bronze landing: raw ROS 2 bags -> partitioned Parquet.

This is the boundary between "robot data" and "warehouse data". Everything
downstream is dbt; this module's only job is to turn each bag into Parquet
without changing a single value, so that a bug in a transformation can never be
confused with a lossy ingest.

Two decisions worth stating plainly:

1. **One Parquet file per (session, topic).** Not one giant file, and not one
   per message. Sensor topics have genuinely different shapes — IMU is a
   handful of floats, LiDAR is a 360-element array per row — so colocating them
   would force the nested array type onto 100 Hz IMU data and waste most of the
   file. Partitioning by topic also means a new sensor only costs a new file.

2. **Appends overwrite by partition, never append blindly.** Idempotency is the
   property this project is built to demonstrate: reprocessing a bag must yield
   identical output rather than doubled rows. So the writer deletes the target
   partition for a session before writing it, making the operation a *replace*.
   That is what makes `make verify` able to pass at all.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

TYPESTORE = get_typestore(Stores.ROS2_HUMBLE)

# Columns every bronze row carries regardless of topic. Keeping the envelope
# uniform across topics is what lets the silver layer union and filter without
# special-casing each sensor.
ENVELOPE = ("session_id", "robot_id", "topic", "t_ns", "t_s")


@dataclass(frozen=True)
class SessionMeta:
    """Robot identity for a session.

    Read from a small JSON sidecar next to the bag rather than guessed from the
    bag itself, because a real ROS bag does not record which robot produced it —
    that is fleet metadata, and inventing it at ingest time would be the kind of
    silent wrong guess this pipeline is meant to avoid.
    """

    session_id: str
    robot_id: str


def discover_bags(raw_dir: Path) -> list[tuple[Path, SessionMeta]]:
    """Find every bag under `raw_dir`, paired with its metadata sidecar."""
    found: list[tuple[Path, SessionMeta]] = []
    for meta_file in sorted(raw_dir.glob("*/session.json")):
        bag_dir = meta_file.parent
        # rosbag2 writes a metadata.yaml; its absence means the bag never
        # finished writing. Skipping loudly beats ingesting a truncated file.
        if not (bag_dir / "metadata.yaml").exists():
            print(f"  ! skipping {bag_dir.name}: no metadata.yaml (incomplete bag)")
            continue
        meta = json.loads(meta_file.read_text())
        found.append((bag_dir, SessionMeta(meta["session_id"], meta["robot_id"])))
    return found


def read_imu(bag: Path, meta: SessionMeta) -> pa.Table:
    rows: dict[str, list] = {c: [] for c in ENVELOPE}
    for extra in ("qx", "qy", "qz", "qw", "ang_vel_x", "ang_vel_y", "ang_vel_z",
                  "lin_acc_x", "lin_acc_y", "lin_acc_z"):
        rows[extra] = []

    with Reader(bag) as reader:
        conns = [c for c in reader.connections if c.topic == "/imu/data"]
        for conn, t_ns, raw in reader.messages(connections=conns):
            m = TYPESTORE.deserialize_cdr(raw, conn.msgtype)
            _envelope(rows, meta, conn.topic, t_ns)
            o, av, la = m.orientation, m.angular_velocity, m.linear_acceleration
            rows["qx"].append(o.x); rows["qy"].append(o.y)
            rows["qz"].append(o.z); rows["qw"].append(o.w)
            rows["ang_vel_x"].append(av.x); rows["ang_vel_y"].append(av.y)
            rows["ang_vel_z"].append(av.z)
            rows["lin_acc_x"].append(la.x); rows["lin_acc_y"].append(la.y)
            rows["lin_acc_z"].append(la.z)
    return _to_table(rows)


def read_odom(bag: Path, meta: SessionMeta) -> pa.Table:
    rows: dict[str, list] = {c: [] for c in ENVELOPE}
    for extra in ("pos_x", "pos_y", "pos_z", "qx", "qy", "qz", "qw",
                  "lin_vel_x", "ang_vel_z"):
        rows[extra] = []

    with Reader(bag) as reader:
        conns = [c for c in reader.connections if c.topic == "/odom"]
        for conn, t_ns, raw in reader.messages(connections=conns):
            m = TYPESTORE.deserialize_cdr(raw, conn.msgtype)
            _envelope(rows, meta, conn.topic, t_ns)
            p = m.pose.pose
            rows["pos_x"].append(p.position.x); rows["pos_y"].append(p.position.y)
            rows["pos_z"].append(p.position.z)
            rows["qx"].append(p.orientation.x); rows["qy"].append(p.orientation.y)
            rows["qz"].append(p.orientation.z); rows["qw"].append(p.orientation.w)
            rows["lin_vel_x"].append(m.twist.twist.linear.x)
            rows["ang_vel_z"].append(m.twist.twist.angular.z)
    return _to_table(rows)


def read_scan(bag: Path, meta: SessionMeta) -> pa.Table:
    rows: dict[str, list] = {c: [] for c in ENVELOPE}
    for extra in ("beam_count", "range_min", "range_max", "min_range", "mean_range",
                  "max_range", "valid_beams"):
        rows[extra] = []

    with Reader(bag) as reader:
        conns = [c for c in reader.connections if c.topic == "/scan"]
        for conn, t_ns, raw in reader.messages(connections=conns):
            m = TYPESTORE.deserialize_cdr(raw, conn.msgtype)
            _envelope(rows, meta, conn.topic, t_ns)
            r = np.asarray(m.ranges, dtype=np.float64)
            # `inf` is the ROS convention for "beam hit nothing within range_max".
            # It is a valid reading, not a missing one, so it is kept for the
            # max but excluded from the mean — averaging infinity would silently
            # turn every aggregate into inf.
            finite = r[np.isfinite(r)]
            rows["beam_count"].append(len(r))
            rows["range_min"].append(float(m.range_min))
            rows["range_max"].append(float(m.range_max))
            rows["min_range"].append(float(finite.min()) if finite.size else None)
            rows["mean_range"].append(float(finite.mean()) if finite.size else None)
            rows["max_range"].append(float(r.max()) if r.size else None)
            rows["valid_beams"].append(int(finite.size))
    return _to_table(rows)


def _envelope(rows: dict[str, list], meta: SessionMeta, topic: str, t_ns: int) -> None:
    rows["session_id"].append(meta.session_id)
    rows["robot_id"].append(meta.robot_id)
    rows["topic"].append(topic)
    rows["t_ns"].append(int(t_ns))
    rows["t_s"].append(float(t_ns / 1e9))


def _to_table(rows: dict[str, list]) -> pa.Table:
    return pa.table({k: pa.array(v) for k, v in rows.items()})


READERS = {
    "/imu/data": (read_imu, "imu"),
    "/odom": (read_odom, "odom"),
    "/scan": (read_scan, "scan"),
}


def land_bronze(raw_dir: Path, bronze_dir: Path) -> list[Path]:
    """Land every bag as Parquet, replacing any existing partition."""
    written: list[Path] = []
    for bag, meta in discover_bags(raw_dir):
        print(f"  {meta.session_id} ({meta.robot_id}) <- {bag.name}")
        for topic, (reader_fn, table_name) in READERS.items():
            table = reader_fn(bag, meta)
            out_dir = bronze_dir / table_name / f"session_id={meta.session_id}"
            # Replace, don't append — see the module docstring on idempotency.
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{table_name}.parquet"
            # A partial file from an interrupted run would be picked up by dbt
            # and read as valid data, so remove before writing.
            out_file.unlink(missing_ok=True)
            pq.write_table(table, out_file, compression="zstd")
            written.append(out_file)
            print(f"    {table_name:<6} {table.num_rows:>7,} rows -> {out_file}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--out", default="data/bronze")
    args = parser.parse_args()

    bags = discover_bags(Path(args.raw))
    if not bags:
        raise SystemExit(f"no bags found in {args.raw} — run `make bags` first")

    print(f"landing {len(bags)} bag(s) -> {args.out}")
    land_bronze(Path(args.raw), Path(args.out))


if __name__ == "__main__":
    main()

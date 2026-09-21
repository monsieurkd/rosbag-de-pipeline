"""Synthetic ROS 2 bag generator.

Why this exists
---------------
The pipeline needs raw robot telemetry in a *real* bag format to be worth
anything — a CSV pretending to be sensor data would prove nothing about
ingesting ROS data. But shipping multi-gigabyte recordings in a portfolio repo
is not an option, and requiring a ROS install would make the project
un-runnable for anyone reviewing it.

So we synthesise bags instead, using `rosbags` (pure Python, no ROS install):
the output is a genuine ROS 2 bag that the official tooling can read, generated
from a seeded simulation, and small enough to regenerate on demand.

The physics is deliberately simple — a differential-drive robot tracing a closed
loop — but it is *consistent*: odometry is derived from the commanded velocity,
and IMU yaw tracks the same heading, so the downstream gold tables can be
validated against each other rather than being four unrelated streams of noise.
That consistency is what makes the data-quality tests in `models/` meaningful.

Usage:
    python -m generate.make_bags --sessions 3 --seed 42
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Writer
from rosbags.typesys import Stores, get_typestore

TYPESTORE = get_typestore(Stores.ROS2_HUMBLE)

# Topic -> (msgtype, nominal publish rate in Hz). Recording what the robot
# actually publishes, at the rates a real robot publishes them: IMU is fast,
# LiDAR is slow, odometry sits in between. Getting these wildly out of
# proportion would make the "sensor health" aggregates meaningless.
TOPICS = {
    "/imu/data": ("sensor_msgs/msg/Imu", 100.0),
    "/odom": ("nav_msgs/msg/Odometry", 20.0),
    "/scan": ("sensor_msgs/msg/LaserScan", 10.0),
}

NS_PER_S = 1_000_000_000

# A closed-loop trajectory for a differential-drive robot: straight, turn,
# straight, turn, ... Parameterised so different seeds produce different but
# equally valid paths.
LOOP_RADIUS_M = 4.0
CRUISE_SPEED_MPS = 1.2
TURN_RATE_RADS = 0.8


@dataclass(frozen=True)
class SessionSpec:
    """One recording session == one bag == one robot run."""

    session_id: str
    robot_id: str
    seed: int
    duration_s: float
    start_ns: int
    imu_archetype: str = "healthy"  # healthy | noisy | failing


def _build_loop_path(spec: SessionSpec) -> tuple[float, float, float, float]:
    """Return (radius, speed, turn_rate, lap_length) scaled by the session seed.

    Scaling by seed means each session is a slightly different run, so the gold
    layer is aggregating genuinely different data rather than three copies of
    the same numbers.
    """
    rng = np.random.default_rng(spec.seed)
    radius = float(LOOP_RADIUS_M * rng.uniform(0.8, 1.25))
    speed = float(CRUISE_SPEED_MPS * rng.uniform(0.85, 1.15))
    turn_rate = float(TURN_RATE_RADS * rng.uniform(0.9, 1.1))
    return radius, speed, turn_rate, 2.0 * math.pi * radius


def generate_session(bag_dir: Path, spec: SessionSpec) -> Path:
    """Write one session's bag to `bag_dir/<session_id>` and return the path."""
    # Clear any previous run BEFORE constructing the Writer: rosbags refuses to
    # write into an existing bag directory, and it checks at construction time,
    # not at write time. Regenerating must replace rather than merge — a
    # half-written directory left over from a crashed run would otherwise fail
    # the whole build, and silently appending to it would make the idempotency
    # guarantee downstream a lie.
    if bag_dir.exists():
        shutil.rmtree(bag_dir)

    radius, speed, turn_rate, lap_len = _build_loop_path(spec)
    rng = np.random.default_rng(spec.seed + 1)

    # Sensor behaviour per archetype. "failing" drops most IMU samples and adds
    # heavy bias so the data-quality/health checks have something real to catch.
    if spec.imu_archetype == "noisy":
        imu_dropout, imu_noise_scale, gyro_bias = 0.02, 4.0, 0.004
    elif spec.imu_archetype == "failing":
        imu_dropout, imu_noise_scale, gyro_bias = 0.35, 9.0, 0.030
    else:
        imu_dropout, imu_noise_scale, gyro_bias = 0.0, 1.0, 0.0

    writer = Writer(bag_dir, version=9)
    writer.open()

    # Now that the Writer has created `bag_dir`, the sidecar can land next to it.
    (bag_dir / "session.json").write_text(
        json.dumps(
            {
                "session_id": spec.session_id,
                "robot_id": spec.robot_id,
                "seed": spec.seed,
                "duration_s": spec.duration_s,
                "start_ns": spec.start_ns,
                "imu_archetype": spec.imu_archetype,
            },
            indent=2,
        )
        + "\n"
    )

    try:
        conns = {
            topic: writer.add_connection(topic, msgtype, typestore=TYPESTORE)
            for topic, (msgtype, _) in TOPICS.items()
        }

        for topic, (msgtype, rate_hz) in TOPICS.items():
            n = int(spec.duration_s * rate_hz)
            step_ns = int(NS_PER_S / rate_hz)

            for i in range(n):
                t_ns = spec.start_ns + i * step_ns
                t_rel = i / rate_hz
                # Convert elapsed time to distance-along-path, then to a pose.
                # Wrapping at lap_len keeps the robot on a closed loop for
                # sessions longer than one lap.
                dist = (speed * t_rel) % lap_len
                theta = dist / radius
                x = radius * math.cos(theta)
                y = radius * math.sin(theta)
                yaw = theta + math.pi / 2.0

                if topic == "/odom":
                    msg = _odom(msgtype, t_ns, x, y, yaw, speed)
                elif topic == "/imu/data":
                    if imu_dropout and rng.random() < imu_dropout:
                        continue  # dropped sample — a real failure mode
                    msg = _imu(msgtype, t_ns, yaw, gyro_bias, imu_noise_scale, rng)
                else:
                    msg = _scan(msgtype, t_ns, x, y, yaw, rng)

                writer.write(conns[topic], t_ns, TYPESTORE.serialize_cdr(msg, msgtype))
    finally:
        writer.close()

    return bag_dir


def _stamp(ts_cls, t_ns: int):
    return ts_cls(sec=t_ns // NS_PER_S, nanosec=t_ns % NS_PER_S)


def _odom(msgtype: str, t_ns: int, x: float, y: float, yaw: float, speed: float):
    """Differential-drive odometry consistent with the commanded trajectory."""
    ts = TYPESTORE.types["builtin_interfaces/msg/Time"]
    header = TYPESTORE.types["std_msgs/msg/Header"]
    pose_wc = TYPESTORE.types["geometry_msgs/msg/PoseWithCovariance"]
    twist_wc = TYPESTORE.types["geometry_msgs/msg/TwistWithCovariance"]
    pose = TYPESTORE.types["geometry_msgs/msg/Pose"]
    point = TYPESTORE.types["geometry_msgs/msg/Point"]
    quat = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
    twist = TYPESTORE.types["geometry_msgs/msg/Twist"]
    vec3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]

    half = yaw / 2.0
    cov = np.zeros(36, dtype=np.float64)
    return TYPESTORE.types[msgtype](
        header=header(stamp=_stamp(ts, t_ns), frame_id="odom"),
        child_frame_id="base_link",
        pose=pose_wc(
            pose=pose(
                position=point(x=x, y=y, z=0.0),
                orientation=quat(x=0.0, y=0.0, z=math.sin(half), w=math.cos(half)),
            ),
            covariance=cov,
        ),
        twist=twist_wc(
            twist=twist(
                linear=vec3(x=speed, y=0.0, z=0.0),
                angular=vec3(x=0.0, y=0.0, z=speed / max(LOOP_RADIUS_M, 1e-6)),
            ),
            covariance=cov,
        ),
    )


def _imu(msgtype: str, t_ns: int, yaw: float, gyro_bias: float, noise_scale: float, rng):
    ts = TYPESTORE.types["builtin_interfaces/msg/Time"]
    header = TYPESTORE.types["std_msgs/msg/Header"]
    quat = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
    vec3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]

    half = yaw / 2.0
    gyro_z = TURN_RATE_RADS + gyro_bias + float(rng.normal(0.0, 0.01 * noise_scale))
    return TYPESTORE.types[msgtype](
        header=header(stamp=_stamp(ts, t_ns), frame_id="imu_link"),
        orientation=quat(x=0.0, y=0.0, z=math.sin(half), w=math.cos(half)),
        orientation_covariance=np.zeros(9, dtype=np.float64),
        angular_velocity=vec3(
            x=float(rng.normal(0.0, 0.01 * noise_scale)),
            y=float(rng.normal(0.0, 0.01 * noise_scale)),
            z=gyro_z,
        ),
        angular_velocity_covariance=np.zeros(9, dtype=np.float64),
        linear_acceleration=vec3(
            x=float(rng.normal(0.0, 0.05 * noise_scale)),
            y=float(rng.normal(0.0, 0.05 * noise_scale)),
            z=9.81 + float(rng.normal(0.0, 0.05 * noise_scale)),
        ),
        linear_acceleration_covariance=np.zeros(9, dtype=np.float64),
    )


def _scan(msgtype: str, t_ns: int, x: float, y: float, yaw: float, rng):
    ts = TYPESTORE.types["builtin_interfaces/msg/Time"]
    header = TYPESTORE.types["std_msgs/msg/Header"]

    n_beams = 360
    angle_min = -math.pi
    angle_increment = 2.0 * math.pi / n_beams
    # Walls of a nominal room around the origin. Range is found by ray-casting
    # the beam against those walls, so the scan is geometrically consistent with
    # where odometry says the robot is.
    room = 6.0
    ranges = np.zeros(n_beams, dtype=np.float32)
    for b in range(n_beams):
        a = yaw + angle_min + b * angle_increment
        dx, dy = math.cos(a), math.sin(a)
        hits = []
        for axis, origin, delta in (("x", x, dx), ("y", y, dy)):
            if abs(delta) < 1e-9:
                continue
            for wall in (-room, room):
                t = (wall - origin) / delta
                if t > 0:
                    other = (y + t * dy) if axis == "x" else (x + t * dx)
                    if -room <= other <= room:
                        hits.append(t)
        ranges[b] = float(min(hits)) if hits else float("inf")

    ranges = ranges + rng.normal(0.0, 0.01, n_beams).astype(np.float32)
    return TYPESTORE.types[msgtype](
        header=header(stamp=_stamp(ts, t_ns), frame_id="laser_frame"),
        angle_min=angle_min,
        angle_max=angle_min + (n_beams - 1) * angle_increment,
        angle_increment=angle_increment,
        time_increment=0.0,
        scan_time=0.1,
        range_min=0.1,
        range_max=12.0,
        ranges=ranges,
        intensities=np.zeros(n_beams, dtype=np.float32),
    )


def default_sessions() -> list[SessionSpec]:
    """Three sessions, one per IMU archetype.

    Having a `failing` session is intentional: a pipeline whose health checks
    only ever see clean data has never been tested. Session 2 is the one that
    should show up as degraded in the gold layer.
    """
    base = 1_700_000_000_000_000_000
    return [
        SessionSpec("session_001", "robot_alpha", seed=42, duration_s=60.0,
                    start_ns=base, imu_archetype="healthy"),
        SessionSpec("session_002", "robot_alpha", seed=43, duration_s=60.0,
                    start_ns=base + 3_600 * NS_PER_S, imu_archetype="failing"),
        SessionSpec("session_003", "robot_beta", seed=44, duration_s=45.0,
                    start_ns=base + 7_200 * NS_PER_S, imu_archetype="noisy"),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/raw", help="raw bag output directory")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    for spec in default_sessions():
        path = generate_session(out / spec.session_id, spec)
        size_mb = sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6
        print(f"wrote {path}  ({size_mb:.1f} MB, imu={spec.imu_archetype})")


if __name__ == "__main__":
    main()

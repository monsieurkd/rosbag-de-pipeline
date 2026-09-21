"""Tests for the bronze landing layer.

These target the property the whole project rests on — that reprocessing a bag
replaces its partition instead of appending to it. If that ever regresses, the
gold layer silently doubles and every aggregate becomes wrong, so it is worth
asserting directly rather than inferring from a passing build.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from generate.make_bags import SessionSpec, generate_session
from ingest.land_bronze import discover_bags, land_bronze


@pytest.fixture
def tiny_session(tmp_path: Path) -> tuple[Path, SessionSpec]:
    """A short bag — 2 seconds is enough to exercise every code path."""
    spec = SessionSpec(
        session_id="test_session",
        robot_id="robot_test",
        seed=7,
        duration_s=2.0,
        start_ns=1_700_000_000_000_000_000,
    )
    bag_dir = generate_session(tmp_path / "raw" / spec.session_id, spec)
    return tmp_path / "raw", spec


def test_generates_a_readable_bag(tiny_session):
    raw_dir, spec = tiny_session
    bag = raw_dir / spec.session_id

    # rosbag2 must have written its metadata for the bag to be considered valid.
    assert (bag / "metadata.yaml").exists()
    assert (bag / "session.json").exists()

    meta = json.loads((bag / "session.json").read_text())
    assert meta["session_id"] == spec.session_id
    assert meta["robot_id"] == spec.robot_id


def test_sidecar_matches_spec(tiny_session):
    """Robot identity travels in the sidecar, not inferred from the bag."""
    raw_dir, spec = tiny_session
    meta = json.loads((raw_dir / spec.session_id / "session.json").read_text())
    # The seed is what makes regeneration reproducible; losing it would make the
    # bags unreproducible without any visible failure.
    assert meta["seed"] == spec.seed


def test_discover_skips_incomplete_bags(tmp_path: Path):
    """A bag directory with no metadata.yaml is a half-written bag."""
    raw = tmp_path / "raw"
    incomplete = raw / "broken_session"
    incomplete.mkdir(parents=True)
    (incomplete / "session.json").write_text(
        json.dumps({"session_id": "broken_session", "robot_id": "r1"})
    )

    assert discover_bags(raw) == []


def test_discover_finds_complete_bags(tiny_session):
    raw_dir, spec = tiny_session
    found = discover_bags(raw_dir)
    assert len(found) == 1
    _, meta = found[0]
    assert meta.session_id == spec.session_id
    assert meta.robot_id == spec.robot_id


def test_landing_produces_expected_partitions(tiny_session, tmp_path: Path):
    raw_dir, spec = tiny_session
    bronze = tmp_path / "bronze"

    land_bronze(raw_dir, bronze)

    for table in ("imu", "odom", "scan"):
        f = bronze / table / f"session_id={spec.session_id}" / f"{table}.parquet"
        assert f.exists(), f"missing {f}"
        assert pq.read_table(f).num_rows > 0


def test_reprocessing_is_idempotent(tiny_session, tmp_path: Path):
    """The property everything else depends on: re-land == same rows, not more.

    Without partition replacement this would land 2x the rows and the failure
    would only surface much later as wrong aggregates.
    """
    raw_dir, spec = tiny_session
    bronze = tmp_path / "bronze"

    land_bronze(raw_dir, bronze)
    first = {
        table: pq.read_table(
            bronze / table / f"session_id={spec.session_id}" / f"{table}.parquet"
        ).num_rows
        for table in ("imu", "odom", "scan")
    }

    land_bronze(raw_dir, bronze)
    second = {
        table: pq.read_table(
            bronze / table / f"session_id={spec.session_id}" / f"{table}.parquet"
        ).num_rows
        for table in ("imu", "odom", "scan")
    }

    assert first == second, f"row counts changed on re-run: {first} -> {second}"
    assert all(n > 0 for n in second.values())


def test_regeneration_is_deterministic(tmp_path: Path):
    """Same seed must produce an identical message stream.

    Checked by landing both generations to Parquet and comparing content, not by
    comparing the bag files byte-for-byte: rosbag2's sqlite3 storage embeds its
    own write timestamps, so the container is never byte-identical even when the
    data is. The message stream is what must be stable, and that is what the
    landed rows capture.
    """
    spec = SessionSpec(
        session_id="det", robot_id="robot_det", seed=99, duration_s=2.0,
        start_ns=1_700_000_000_000_000_000,
    )

    a = generate_session(tmp_path / "a" / "det", spec)
    b = generate_session(tmp_path / "b" / "det", spec)

    bronze_a = land_bronze(a.parent, tmp_path / "bronze_a")
    bronze_b = land_bronze(b.parent, tmp_path / "bronze_b")

    assert len(bronze_a) == len(bronze_b)

    # Compare the actual values, not just row counts — same shape with different
    # numbers would still be a determinism failure.
    for file_a, file_b in zip(sorted(bronze_a), sorted(bronze_b)):
        assert pq.read_table(file_a).equals(pq.read_table(file_b)), (
            f"{file_a.name} differs between two generations of the same seed"
        )

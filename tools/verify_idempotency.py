"""Gold export + comparison, used by `make verify`.

The README claims the build is idempotent: re-running the pipeline over the same
bags produces identical gold tables. That claim is worthless as prose — it is
exactly the kind of assertion that quietly stops being true. So it is checked
mechanically instead: `make verify` builds the warehouse twice into two separate
DuckDB files and compares the exported gold tables.

Exports are written as JSON with sorted keys and sorted rows so the comparison
has nothing to disagree about except real differences. A comparison that fails
on row order would train the reader to ignore it.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import duckdb

GOLD_TABLES = ("gold_session_health", "gold_robot_summary")
GOLD_SCHEMA = "main_gold"


def export_gold(db_path: Path, out_dir: Path) -> dict[str, str]:
    """Write each gold table to CSV and return {table: sha256}."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    digests: dict[str, str] = {}

    try:
        for table in GOLD_TABLES:
            fqn = f"{GOLD_SCHEMA}.{table}"
            rows = con.sql(f"select * from {fqn}").fetchall()
            headers = [d[0] for d in con.sql(f"select * from {fqn} limit 0").description]

            # Sort rows so the digest reflects content, not physical order.
            # DuckDB gives no ordering guarantee without ORDER BY, and a
            # different scan order between runs is not a real difference.
            rows = sorted(rows, key=lambda r: [str(v) for v in r])

            out_file = out_dir / f"{table}.csv"
            with out_file.open("w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(headers)
                writer.writerows(rows)

            digest = hashlib.sha256(out_file.read_bytes()).hexdigest()
            digests[table] = digest
            print(f"  {table:<22} {len(rows):>4} rows  sha256={digest[:16]}…")
    finally:
        con.close()

    return digests


def diff_gold(a_dir: Path, b_dir: Path) -> bool:
    """Compare two gold exports. Returns True when identical."""
    ok = True
    for table in GOLD_TABLES:
        a_file, b_file = a_dir / f"{table}.csv", b_dir / f"{table}.csv"
        a = a_file.read_bytes()
        b = b_file.read_bytes()
        if a == b:
            print(f"  ✓ {table} identical")
            continue

        ok = False
        print(f"  ✗ {table} DIFFERS")
        a_rows = list(csv.reader(a.decode().splitlines()))
        b_rows = list(csv.reader(b.decode().splitlines()))
        print(f"      rows: {len(a_rows)} vs {len(b_rows)}")
        # Report the first differing row rather than dumping both tables — the
        # useful information is which row and column diverged, not the volume
        # of output.
        for i, (ra, rb) in enumerate(zip(a_rows, b_rows)):
            if ra != rb:
                for col, (va, vb) in enumerate(zip(ra, rb)):
                    if va != vb:
                        print(f"      row {i}, col {col}: {va!r} != {vb!r}")
                break
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_subparsers(dest="mode", required=True)

    p_export = mode.add_parser("export")
    p_export.add_argument("--db", required=True)
    p_export.add_argument("--out", required=True)

    p_diff = mode.add_parser("diff")
    p_diff.add_argument("--a", required=True)
    p_diff.add_argument("--b", required=True)

    args = parser.parse_args()

    if args.mode == "export":
        digests = export_gold(Path(args.db), Path(args.out))
        (Path(args.out) / "digests.json").write_text(json.dumps(digests, indent=2) + "\n")
    else:
        ok = diff_gold(Path(args.a), Path(args.b))
        if not ok:
            raise SystemExit("\n✗ gold layer is NOT idempotent — see differences above")
        print("\n✓ idempotent: both rebuilds produced identical gold tables")


if __name__ == "__main__":
    main()

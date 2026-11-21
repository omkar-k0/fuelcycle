"""End-to-end tests on a small synthetic sample (CI). Real-data numbers live in results/."""
import json
import pathlib
import subprocess
import sys

import duckdb
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("fc")
    raw, bronze, db = tmp / "raw", tmp / "bronze", tmp / "w.duckdb"
    subprocess.run([sys.executable, "scripts/make_sample.py", str(raw), "16", "80"], cwd=ROOT, check=True)
    subprocess.run([sys.executable, "-m", "fuelcycle.pipeline", "--raw", str(raw), "--bronze", str(bronze),
                    "--db", str(db), "--full-refresh",
                    "--vars", '{"lf_follow_minutes": 30, "lf_fuel": "e5", "hourly_marts": true}'], cwd=ROOT, check=True)
    return tmp, raw, bronze, db


def test_ingest_is_idempotent(built):
    from fuelcycle import ingest
    _, raw, bronze, _ = built
    stats = ingest.ingest(raw, bronze)
    assert stats["loaded"] == 0 and stats["skipped"] > 0


def test_every_bronze_event_reaches_silver(built):
    *_, db = built
    con = duckdb.connect(str(db), read_only=True)
    n_silver = con.execute("select count(*) from silver_price_events").fetchone()[0]
    n_bronze = con.execute("""select count_if(dieselchange in (1,3) or dieselchange is null)
                                + count_if(e5change in (1,3) or e5change is null)
                                + count_if(e10change in (1,3) or e10change is null) from stg_prices""").fetchone()[0]
    assert n_silver == n_bronze


def test_hours_complete_and_not_past_data(built):
    *_, db = built
    con = duckdb.connect(str(db), read_only=True)
    n_hours = con.execute("select count(distinct hour_ts) from gold_area_hour").fetchone()[0]
    assert n_hours == 16 * 24
    n_days = con.execute("select count(distinct day) from gold_area_day").fetchone()[0]
    assert n_days == 16


def test_planted_leaders_lead(built):
    # the sample generator makes ARAL and Shell move first; the mart should find that
    *_, db = built
    con = duckdb.connect(str(db), read_only=True)
    lead, other = con.execute("""
        select avg(lead_share) filter (where brand in ('ARAL','Shell')),
               avg(lead_share) filter (where brand not in ('ARAL','Shell'))
        from gold_leader_follower where lead_share is not null""").fetchone()
    assert lead > other + 0.1


def test_cheapest_hour_abstains_without_enough_days(built):
    *_, db = built
    con = duckdb.connect(str(db), read_only=True)
    decisions = {r[0] for r in con.execute("select distinct decision from gold_cheapest_hour").fetchall()}
    assert decisions == {"not enough days"}  # 16 days < cheapest_min_days (21)


def test_fault_injection_scores(built, tmp_path):
    _, raw, *_ = built
    out = tmp_path / "dq.json"
    subprocess.run([sys.executable, "-m", "fuelcycle.eval_dq", "--raw", str(raw), "--work", str(tmp_path / "w"),
                    "--days", "4", "--rate", "0.01", "--out", str(out)], cwd=ROOT, check=True)
    t = json.loads(out.read_text())["totals"]
    assert t["injected"] > 50
    assert t["recall"] == 1.0 and t["precision"] == 1.0

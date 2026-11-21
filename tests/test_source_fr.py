import gzip
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_french_archive_to_canonical_raw(tmp_path):
    out = tmp_path / "raw"
    subprocess.run([sys.executable, "-m", "fuelcycle.sources.fr", "tests/fixtures/fr_mini_2025.zip",
                    "--out", str(out), "--start", "2025-11-01", "--end", "2025-11-30"], cwd=ROOT, check=True)
    files = sorted(p.name for p in (out / "prices").glob("*.csv.gz"))
    # 30 Oct is outside the window, SP98 has no column, the garbage timestamp is kept for the gate
    assert files == ["2025-11-01.unparsed.csv.gz", "2025-11-03.csv.gz", "2025-11-04.csv.gz"]
    rows = gzip.open(out / "prices" / "2025-11-03.csv.gz", "rt").read().splitlines()
    assert rows[0] == "date,station_uuid,diesel,e5,e10,dieselchange,e5change,e10change"
    assert "2025-11-03 07:53:00+01,1000001,,1.789,,0,1,0" in rows  # millièmes converted to EUR
    stations = gzip.open(next((out / "stations").glob("*.csv.gz")), "rt").read()
    assert "SAINT-DENIS-LÈS-BOURG" in stations and "46.20114" in stations

    from fuelcycle import ingest
    stats = ingest.ingest(out, tmp_path / "bronze")
    assert stats["loaded"] == 4  # 3 price files (one is the unparsed bucket) + 1 station file

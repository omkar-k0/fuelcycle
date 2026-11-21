"""Download Tankerkoenig price history (CC BY-NC-SA 4.0) as gzipped CSV.

Usage: python3 scripts/download.py [START YYYY-MM-DD] [END YYYY-MM-DD]
Default: 2025-11-01 .. 2026-10-31 (12 months). Safe to re-run: existing files are skipped.
"""
import concurrent.futures as cf
import datetime as dt
import gzip
import pathlib
import shutil
import sys
import urllib.error
import urllib.request

BASE = ("https://dev.azure.com/tankerkoenig/tankerkoenig-data/_apis/git/"
        "repositories/tankerkoenig-data/items?api-version=7.1&download=true&path=")
RAW = pathlib.Path("data/raw")


def fetch(kind: str, day: dt.date) -> str:
    rel = f"{kind}/{day:%Y}/{day:%m}/{day:%Y-%m-%d}-{kind}.csv"
    out = RAW / kind / f"{day:%Y-%m-%d}.csv.gz"
    if out.exists() and out.stat().st_size > 1000:
        return f"skip {rel}"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".part")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(BASE + "/" + rel, timeout=120) as r, gzip.open(tmp, "wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
            with gzip.open(tmp, "rt", errors="replace") as chk:
                head = chk.read(40)
            if not head.startswith(("date,station_uuid", "uuid,")):
                tmp.unlink()
                return f"BAD  {rel}: got something that is not a CSV (login page?): {head[:30]!r}"
            tmp.rename(out)
            return f"ok   {rel} {out.stat().st_size/1e6:.1f} MB"
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return f"404  {rel}"
            err = e
        except Exception as e:  # network hiccup, retry
            err = e
    return f"FAIL {rel}: {err}"


def main() -> None:
    start = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date(2025, 11, 1)
    end = dt.date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else dt.date(2026, 10, 31)
    free = shutil.disk_usage(".").free / 1e9
    print(f"free disk: {free:.0f} GB")
    if free < 15:
        sys.exit("Less than 15 GB free, stopping.")
    probe = fetch("prices", start)
    print("probe:", probe)
    if not probe.startswith(("ok", "skip")):
        sys.exit("Probe failed, send this output to Claude.")
    days = [start + dt.timedelta(d) for d in range((end - start).days + 1)]
    jobs = [("prices", d) for d in days] + [("stations", start), ("stations", end)]
    with cf.ThreadPoolExecutor(6) as ex:
        for i, msg in enumerate(ex.map(lambda j: fetch(*j), jobs), 1):
            print(f"[{i}/{len(jobs)}] {msg}", flush=True)
    print("done")


if __name__ == "__main__":
    main()

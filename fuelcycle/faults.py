"""Inject known faults into a copy of raw price files and write the ground-truth labels.

Fault types match the reason codes of the DQ gate. Each injected fault is labelled with the
event ids it should affect (source_file:source_row:fuel), so the gate can be scored.
"""
import csv
import gzip
import io
import pathlib
import random
import shutil
import uuid

FUELS = ("diesel", "e5", "e10")
FAULTS = ("nonpositive_price", "price_out_of_range", "unknown_station", "timestamp_outside_file_day",
          "duplicate", "unparseable", "spike_reversal", "regional_outlier")


def _events_of(row: dict) -> list[str]:
    return [f for f in FUELS if row[f + "change"] in ("1", "3")]


def _shift_day(ts: str) -> str:
    # "2026-01-05 10:05:23+01" -> same time one day later; a clock-skew style fault
    import datetime as dt
    d = dt.datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S") + dt.timedelta(days=1)
    return d.strftime("%Y-%m-%d %H:%M:%S") + ts[19:]


def _plus_minutes(ts: str, m: int) -> str:
    import datetime as dt
    d = dt.datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S") + dt.timedelta(minutes=m)
    if d.date().isoformat() != ts[:10]:
        return ts
    return d.strftime("%Y-%m-%d %H:%M:%S") + ts[19:]


def inject_file(src: pathlib.Path, dst: pathlib.Path, rate: float, rng: random.Random) -> list[dict]:
    with gzip.open(src, "rt", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)
    out_rows: list[dict] = []
    pending: list[tuple[str, dict, list[str]]] = []  # (fault, row, fuels) to label after numbering
    for row in rows:
        fuels = _events_of(row)
        if not fuels or rng.random() > rate:
            out_rows.append((row, None, None))
            continue
        fault = rng.choice(FAULTS)
        fuel = rng.choice(fuels)
        r = dict(row)
        if fault == "nonpositive_price":
            r[fuel] = rng.choice(["0.000", "-0.001"])
            out_rows.append((r, fault, [fuel]))
        elif fault == "price_out_of_range":
            p = float(r[fuel])
            r[fuel] = f"{rng.choice([p * 10, p / 10]):.3f}"  # decimal slip
            out_rows.append((r, fault, [fuel]))
        elif fault == "unknown_station":
            r["station_uuid"] = str(uuid.UUID(int=rng.getrandbits(128)))
            out_rows.append((r, fault, fuels))
        elif fault == "timestamp_outside_file_day":
            r["date"] = _shift_day(r["date"])
            out_rows.append((r, fault, fuels))
        elif fault == "duplicate":
            out_rows.append((row, None, None))
            out_rows.append((dict(row), fault, fuels))
        elif fault == "unparseable":
            if rng.random() < 0.5:
                r[fuel] = rng.choice(["1,749", "n/a", ""])
                out_rows.append((r, fault, [fuel]))
            else:
                r["date"] = "2026-13-45 99:99:99+01"
                out_rows.append((r, fault, fuels))
        elif fault == "spike_reversal":
            # real row, then a +15% spike a few minutes later that reverts a few minutes after that
            out_rows.append((row, None, None))
            spike = {**row, "date": _plus_minutes(row["date"], 7)}
            back = {**row, "date": _plus_minutes(row["date"], 14)}
            for f in FUELS:
                spike[f + "change"] = "0"
                back[f + "change"] = "0"
            spike[fuel] = f"{float(row[fuel]) * 1.15:.3f}"
            spike[fuel + "change"] = "1"
            back[fuel + "change"] = "1"
            if spike["date"] == row["date"] or back["date"] == spike["date"]:
                continue  # too close to midnight, skip this one
            out_rows.append((spike, fault, [fuel]))
            out_rows.append((back, "collateral", [fuel]))
        elif fault == "regional_outlier":
            r[fuel] = f"{float(r[fuel]) + rng.choice([-1, 1]) * rng.uniform(0.45, 0.70):.3f}"
            out_rows.append((r, fault, [fuel]))

    labels = []
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=header, lineterminator="\n")
    w.writeheader()
    for i, (r, fault, fuels) in enumerate(out_rows, 1):
        w.writerow(r)
        if fault:
            labels += [{"event_id": f"{dst.name}:{i}:{f}", "fault": fault} for f in fuels]
    dst.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(dst, "wt", newline="") as f:
        f.write(buf.getvalue())
    return labels


def inject(raw_in: pathlib.Path, raw_out: pathlib.Path, days: list[str], rate: float, seed: int) -> list[dict]:
    rng = random.Random(seed)
    labels = []
    for d in days:
        labels += inject_file(raw_in / "prices" / f"{d}.csv.gz", raw_out / "prices" / f"{d}.csv.gz", rate, rng)
    (raw_out / "stations").mkdir(parents=True, exist_ok=True)
    for s in (raw_in / "stations").glob("*.csv.gz"):
        shutil.copy(s, raw_out / "stations" / s.name)
    return labels

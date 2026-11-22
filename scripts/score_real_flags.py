"""Score the gate's flags on real data against labels (portfolio plan P4).

Inputs, all in labels/:
- real_flags_sheet.csv: 100 flags drawn at random (fixed seed) from the 7,050 real flags, without reasons
- real_flags_key.csv: the reason code the gate gave each one (kept apart while labelling)
- real_flags_labels_omkar.csv: primary labels (Gemini drafted a first pass; Omkar checked every row)
- real_flags_labels_sonnet.csv: second, independent labels by Claude Sonnet 5.5, given only the sheet

Precision = error / (error + real); unsure is reported beside it, not counted.
Writes results/real_flags_eval.json.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "labels"


def read(name: str, col: str) -> dict[int, str]:
    with open(LAB / name, newline="") as f:
        return {int(r["item"]): r[col].strip() for r in csv.DictReader(f)}


def precision(labels: dict[int, str], items) -> dict:
    c = Counter(labels[i] for i in items)
    judged = c["error"] + c["real"]
    return {
        "n": len(items), "error": c["error"], "real": c["real"], "unsure": c["unsure"],
        "precision": round(c["error"] / judged, 3) if judged else None,
    }


def kappa(a: dict[int, str], b: dict[int, str]) -> float:
    items = sorted(a)
    n = len(items)
    po = sum(a[i] == b[i] for i in items) / n
    ca, cb = Counter(a.values()), Counter(b.values())
    pe = sum(ca[k] / n * cb[k] / n for k in set(ca) | set(cb))
    return round((po - pe) / (1 - pe), 3)


def main() -> None:
    key = read("real_flags_key.csv", "dq_reasons")
    omkar = read("real_flags_labels_omkar.csv", "label")
    sonnet = read("real_flags_labels_sonnet.csv", "label")
    assert set(key) == set(omkar) == set(sonnet), "label files must cover the same items"
    items = sorted(key)
    reasons = sorted({k for k in key.values()})
    out = {
        "sample": "100 of 7,050 real flags, random with fixed seed",
        "primary_labels": "Omkar (Gemini drafted a first pass; every row checked by Omkar)",
        "second_labels": "Claude Sonnet 5.5, blind: sheet only, no reasons, no primary labels",
        "primary": precision(omkar, items),
        "second": precision(sonnet, items),
        "by_reason_primary": {r: precision(omkar, [i for i in items if key[i] == r]) for r in reasons},
        "agreement": {
            "same_label": sum(omkar[i] == sonnet[i] for i in items),
            "cohen_kappa": kappa(omkar, sonnet),
            "disagreements": [{"item": i, "primary": omkar[i], "second": sonnet[i]} for i in items if omkar[i] != sonnet[i]],
        },
    }
    path = ROOT / "results" / "real_flags_eval.json"
    path.write_text(json.dumps(out, indent=1) + "\n")
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()

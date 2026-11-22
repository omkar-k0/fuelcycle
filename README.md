# fuelcycle

**Every fuel price change at French petrol stations, run through a data-quality gate, modelled in dbt on DuckDB, on one laptop.**

Live demo: https://fuelcycle-seven.vercel.app · Data: prix-carburants.gouv.fr (Licence Ouverte 2.0)

## Results (12 months, 1 Nov 2025 to 31 Oct 2026)

| | |
|---|---|
| Price events | 3,297,846 (Gazole, SP95, E10) from 10,150 active stations |
| Full rebuild | 54 s (silver 35 s, marts and tests 19 s) on a 4-core, 3 GB RAM Linux VM |
| Bronze = silver reconciliation | 3,297,846 = 3,297,846, test passes |
| Gate on real data | 7,050 events flagged (0.21%), 0 quarantined |

**Data-quality gate, fault injection on 14 real days** (586 injected faults, `results/dq_eval.json`)

| Injected fault | Injected | Caught | Right reason |
|---|---:|---:|---:|
| duplicate | 67 | 100% | 100% |
| nonpositive_price | 65 | 100% | 100% |
| price_out_of_range | 81 | 100% | 100% |
| regional_outlier | 80 | 97.5% | 97.5% |
| spike_reversal | 79 | 100% | 100% |
| timestamp_outside_file_day | 70 | 100% | 100% |
| unknown_station | 62 | 100% | 100% |
| unparseable | 82 | 100% | 100% |
| **All** | **586** | **99.7%** | |

- **Precision 99.7%:** 2 false alarms among 118,795 clean events.
- **Not scored:** 264 events that the gate already flags on the untouched copy. They are real anomalies whose truth is unknown.
- **What this measures:** the fault catalogue was written alongside the rules, so it shows the rules are implemented correctly and how few false alarms they raise on real data. It does not show that they find fault types nobody thought of.
- **On real data, most flags are genuine prices.** 100 of the 7,050 real flags, drawn at random, were labelled blind (`labels/`, `results/real_flags_eval.json`, `python3 scripts/score_real_flags.py`):

| Labeller | Real error | Genuine price | Unsure | Precision |
|---|---:|---:|---:|---:|
| Omkar (Gemini drafted a first pass; every row checked by Omkar) | 12 | 86 | 2 | 12.2% |
| Claude Sonnet 5.5, second check, blind | 13 | 86 | 1 | 13.1% |

  - The two agree on 99 of 100 rows (Cohen's kappa 0.96). The one difference: item 52, unsure vs error.
  - `regional_outlier` (97 of the 100) is right on 9 of 95 judged rows (9.5%). It mostly flags stations that are simply expensive: Paris, motorways, islands, price caps held for days.
  - `spike_reversal` was right on all 3 of its rows. Three rows are too few to claim a rate.
  - **What this means:** the gate catches planted errors (99.7%) but the regional rule raises far more alarms than real errors. Fixing it needs a station's own price history, not only its neighbours; not done yet.

**Next-day forecast, rolling-origin backtest** (8 weeks, 5 Sep to 30 Oct 2026, 19,679 area-fuel-days, `results/forecast_eval.json`)

| | Model | Baseline |
|---|---:|---:|
| Error on tomorrow's price (MAE) | 0.709 ct | 0.712 ct (tomorrow = today) |
| 80% band contains tomorrow's price | 80.8% | 76.4% before calibration |
| Days it answers instead of "uncertain" | 3.4% | – |
| "Fill now" right (price did not fall) | 92% | 72% if you always fill now |
| "Wait" right (price fell) | 44% | 28% if you always wait |

- **Point forecast:** day-to-day French prices are close to a random walk, so the forecast is no better than "tomorrow = today".
- **The band:** calibrated and honest.
- **"Fill now":** beats its base rate by 20 points.
- **"Wait":** beats its base rate, but is still wrong more often than right, so it shouldn't be shown to users.
- **Caveat:** that decision was made after seeing this test window, so it has to be confirmed on the next months of data before it counts. The window is a single 8-week period that included a price peak.

## What the real data changed

1. **DuckDB's `UNPIVOT` drops NULLs by default.**
   - **Found by:** the reconciliation test failed on injected rows with an unreadable price, which were vanishing instead of being quarantined.
   - **Fix:** `UNPIVOT INCLUDE NULLS`.
2. **Motorway stations are expensive, not wrong.**
   - **Found:** in the first full run, 16,522 of 20,418 flags (81%) hit the 477 motorway stations.
   - **Fix:** the outlier rule now compares each station with peers of the same station type in its region. Flags dropped to 5,064.
3. **Peer groups that are too small hide outliers.**
   - **Found:** grouping peers by hour as well left many events with one or two peers, so the median was the event itself. Recall on injected outliers was 68.8% (`results/dq_eval_v1_hourly_peers.json`).
   - **Fix:** peers are now the whole day, and the rule abstains when there are fewer than 5 comparable stations. Recall rose to 97.5%; real flags settled at 7,050.
4. **An hourly grid doesn't fit a daily market.**
   - **Found:** in November 2025, 86% of station-fuel-days had exactly one price change. An hourly grid per city spilled about 2 GB per month on a 3 GB machine and added no information.
   - **Fix:** area marts are daily. The hourly marts (`hourly_marts: true`) stay for markets like Germany, where stations reprice several times a day.
5. **`approx_quantile` is not always cheaper.** One t-digest per group across ~300k groups ran out of memory, while exact quantiles spill to disk.

## What it does

| Layer | What it holds | How |
|---|---|---|
| Source adapter | French yearly XML → canonical daily CSV | `fuelcycle/sources/fr.py`. Streams the XML, converts millièmes to EUR, attaches the Europe/Paris offset, and keeps rows with unreadable timestamps for the gate. |
| Bronze | every source row, untouched, one Parquet file per day | `fuelcycle/ingest.py`: values are read as text and cast with `TRY_CAST`; a manifest stores checksums so re-runs skip unchanged files; files that are not price data are refused. |
| Silver | one row per price event, with a DQ decision | `dbt/models/silver/silver_price_events.sql`: incremental, one month per run |
| Gold | daily prices per area, change timing, leader/follower | `dbt/models/gold/` |
| Forecast | tomorrow's price with a calibrated 80% band, gated decision | `fuelcycle/forecast.py` |

The canonical raw format is the layout of Germany's Tankerkönig feed. A second country is one adapter, not a new pipeline. Tankerkönig's history now needs a verified account (registration was paused when this was built), so Germany is the next adapter, not part of these results.

### The data-quality gate

Every price event ends up in exactly one of three states:
- **quarantined**, excluded downstream: `unparseable`, `nonpositive_price`, `price_out_of_range` (outside 0.80–3.50 EUR), `unknown_station`, `timestamp_outside_file_day`, `duplicate`
- **flagged**, kept but not used by the marts: `spike_reversal` (a jump of more than 8% that reverts within 60 min), `regional_outlier` (more than max(30 ct, 6 MAD) from the median of the same day, fuel, postcode region and station type; abstains with fewer than 5 peers)
- **accepted**

`dbt/tests/assert_bronze_reconciles.sql` checks bronze price cells = silver events, per day.

**How the gate is scored** (`fuelcycle/eval_dq.py`): 14 days spread over the year are copied twice, and one copy gets labelled faults. Both copies go through ingest and silver.
- *Caught*: flagged or quarantined.
- *Right reason*: the matching reason code.
- *False alarm*: accepted in the clean copy, not accepted in the faulty one, and not an injected fault.

### Gold marts

- `gold_area_day`: the price in force at 12:00 per station and fuel (an ASOF join), rolled up to France, the 95 départements and 135 cities with at least 10 stations. Station-fuels silent for 7 days are treated as closed.
- `gold_change_timing`: when in the day stations change prices, and the share of rises.
- `gold_leader_follower`: for station pairs within 2 km, a move by A counts as followed if B moves the same way within 12 h and before A moves again.
  - **Result on French E10:** leaders and followers come out in about equal numbers (median lead share 0.5).
  - **Caveat:** that could be chance from everyone repricing overnight. These labels need a shuffled-time baseline before they mean anything.
- `gold_area_hour`, `gold_daily_cycle`, `gold_cheapest_hour`: hourly marts, off for France (`hourly_marts: false`).

## Run it

```bash
python3 -m venv .venv && source .venv/bin/activate
make setup
mkdir -p data/fr
curl -fL -o data/fr/2025.zip https://donnees.roulez-eco.fr/opendata/annee/2025
curl -fL -o data/fr/2026.zip https://donnees.roulez-eco.fr/opendata/annee/2026
python -m fuelcycle.sources.fr data/fr/2025.zip data/fr/2026.zip --out data/raw --start 2025-11-01 --end 2026-10-31
make all         # pipeline, DQ eval, forecast backtest, site export
make test        # end-to-end tests on a synthetic sample (what CI runs)
```

`FUELCYCLE_MEMORY` caps DuckDB memory (default 6GB; the results above used 1500MB).

## Repo layout

```
fuelcycle/sources/fr.py    French archive -> canonical raw files
scripts/download.py        German Tankerkoenig history (needs a verified account)
scripts/make_sample.py     synthetic sample in the same format, for CI only
fuelcycle/ingest.py        raw -> bronze Parquet + manifest
fuelcycle/pipeline.py      orchestration: dbt month by month
fuelcycle/faults.py        fault injection with labels
fuelcycle/eval_dq.py       scores the DQ gate
fuelcycle/forecast.py      quantile forecast, conformal band, backtest, gated decision
fuelcycle/export_site.py   aggregates -> site/data/fuelcycle.json
dbt/                       staging, silver (DQ gate), gold marts, tests
site/                      static demo page (Vercel)
results/                   eval outputs from the real run
```

## Data licence

Price data: prix-carburants.gouv.fr, Ministère de l'Économie, Licence Ouverte / Open Licence 2.0. It isn't stored in this repo, apart from the aggregates in `site/data/`. Code: MIT.

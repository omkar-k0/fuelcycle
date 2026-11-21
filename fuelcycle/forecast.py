"""Tomorrow's price per area, with a calibrated 80% band, and a gated "fill up today or wait?".

Grain: daily (gold_area_day, the price in force at 12:00). At the end of day t we know prices up
to t and predict the price at t+1 for Germany/France as a whole and every larger city, per fuel.

Models
- naive:      tomorrow = today (random walk); the baseline to beat
- naive_week: tomorrow = today + the change seen on the same weekday last week
- lgbm:       LightGBM quantile models (p10, p50, p90) on tomorrow's change; the 80% band is
              calibrated on the most recent held-out week (conformalized quantile regression)

Decision per area, fuel and day:
- "wait"     if the whole band for tomorrow is below today's price (p90 < today)
- "fill now" if the whole band is above today's price (p10 > today)
- otherwise "uncertain"
Rolling-origin backtest: retrain before every test week.
"""
import argparse
import json
import pathlib

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

QUANTILES = {"p10": 0.1, "p50": 0.5, "p90": 0.9}
FEATS = ["weekday_next", "d1", "d2", "d3", "d7", "chg_7d", "chg_28d", "vs_mean_7d", "vs_mean_28d",
         "spread_vs_country", "country_d1", "n_stations", "series_cat", "fuel_cat"]


def load(db: str, fuels: tuple[str, ...]) -> pd.DataFrame:
    con = duckdb.connect(db, read_only=True)
    df = con.execute(f"""
        select day, level, area, fuel, price_mean as price, n_stations
        from gold_area_day
        where level in ('country', 'city') and fuel in {fuels}
        order by level, area, fuel, day
    """).df()
    df["day"] = pd.to_datetime(df.day)
    df["series"] = df.level + "|" + df.area + "|" + df.fuel
    return df


def features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["series", "day"]).copy()
    g = df.groupby("series", sort=False).price
    df["target"] = g.shift(-1) - df.price                 # tomorrow's change
    df["price_next"] = g.shift(-1)
    df["next_day"] = df.groupby("series", sort=False).day.shift(-1)
    for k in (1, 2, 3, 7):
        df[f"d{k}"] = df.price - g.shift(k) if k == 1 else g.shift(k - 1) - g.shift(k)
    df["chg_7d"] = df.price - g.shift(7)
    df["chg_28d"] = df.price - g.shift(28)
    df["vs_mean_7d"] = df.price - g.transform(lambda s: s.rolling(7, min_periods=5).mean())
    df["vs_mean_28d"] = df.price - g.transform(lambda s: s.rolling(28, min_periods=20).mean())
    country = df[df.level == "country"][["day", "fuel", "price", "d1"]].rename(
        columns={"price": "country_price", "d1": "country_d1"})
    df = df.merge(country, on=["day", "fuel"], how="left")
    df["spread_vs_country"] = df.price - df.country_price
    df["weekday_next"] = (df.day + pd.Timedelta(days=1)).dt.weekday
    df["series_cat"] = df.series.astype("category")
    df["fuel_cat"] = df.fuel.astype("category")
    # only consecutive days count as "tomorrow"
    ok = (df.next_day - df.day) == pd.Timedelta(days=1)
    df.loc[~ok, ["target", "price_next"]] = np.nan
    return df


def fit_predict(fit: pd.DataFrame, frames: list[pd.DataFrame]) -> None:
    for name, q in QUANTILES.items():
        m = lgb.LGBMRegressor(objective="quantile", alpha=q, n_estimators=300, learning_rate=0.05,
                              num_leaves=31, min_child_samples=40, subsample=0.8, subsample_freq=1,
                              colsample_bytree=0.9, verbose=-1)
        m.fit(fit[FEATS], fit.target)
        for f in frames:
            f[name] = f.price + m.predict(f[FEATS])


def backtest(df: pd.DataFrame, test_weeks: int) -> pd.DataFrame:
    df = df.dropna(subset=["target", "chg_28d", "vs_mean_28d", "d7"]).copy()
    days = np.sort(df.day.unique())
    test_start = days[-test_weeks * 7]
    out = []
    for w in range(test_weeks):
        t0 = test_start + np.timedelta64(7 * w, "D")
        t1 = t0 + np.timedelta64(7, "D")
        # the target of day t is known on t+1, so training rows must end the day before t0
        train = df[df.day < t0 - np.timedelta64(1, "D")]
        test = df[(df.day >= t0) & (df.day < t1)].copy()
        if test.empty:
            continue
        cal_start = t0 - np.timedelta64(8, "D")
        fit, cal = train[train.day < cal_start - np.timedelta64(1, "D")], train[train.day >= cal_start].copy()
        fit_predict(fit, [cal, test])
        score = np.maximum(cal.p10 - cal.price_next, cal.price_next - cal.p90)
        n = len(score)
        qhat = float(np.quantile(score, min(1.0, np.ceil((n + 1) * 0.8) / n)))
        test["p10_raw"], test["p90_raw"] = test.p10, test.p90
        test["p10"], test["p90"] = test.p10 - qhat, test.p90 + qhat
        test[["p10", "p50", "p90"]] = np.sort(test[["p10", "p50", "p90"]].values, axis=1)
        test["qhat"] = qhat
        test["naive"] = test.price
        test["naive_week"] = test.price + test.d7.fillna(0)
        out.append(test)
    bt = pd.concat(out)
    bt["decision"] = np.where(bt.p90 < bt.price, "wait", np.where(bt.p10 > bt.price, "fill now", "uncertain"))
    return bt


def summarise(bt: pd.DataFrame) -> dict:
    def mae(col):
        return round(float((bt[col] - bt.price_next).abs().mean() * 100), 3)
    moved = bt.price_next - bt.price
    wait, fill = bt[bt.decision == "wait"], bt[bt.decision == "fill now"]
    answered = bt[bt.decision != "uncertain"]
    correct = ((answered.decision == "wait") & (answered.price_next < answered.price)) | \
              ((answered.decision == "fill now") & (answered.price_next >= answered.price))
    return {
        "test_rows": int(len(bt)), "series": int(bt.series.nunique()),
        "test_days": [str(bt.day.min().date()), str(bt.day.max().date())],
        "share_days_price_moved": round(float((moved.abs() >= 0.001).mean()), 3),
        "mae_ct": {"lgbm_p50": mae("p50"), "naive_tomorrow_equals_today": mae("naive"),
                   "naive_same_weekday_change": mae("naive_week")},
        "interval_80_raw": {"coverage": round(float(((bt.price_next >= bt.p10_raw) & (bt.price_next <= bt.p90_raw)).mean()), 3)},
        "interval_80": {"coverage": round(float(((bt.price_next >= bt.p10) & (bt.price_next <= bt.p90)).mean()), 3),
                         "mean_width_ct": round(float((bt.p90 - bt.p10).mean() * 100), 2)},
        "decision": {
            "share": {k: round(float(v), 3) for k, v in bt.decision.value_counts(normalize=True).items()},
            "answered_correct": round(float(correct.mean()), 3) if len(answered) else None,
            "wait_correct": round(float((wait.price_next < wait.price).mean()), 3) if len(wait) else None,
            "wait_mean_saving_ct": round(float(((wait.price - wait.price_next) * 100).mean()), 2) if len(wait) else None,
            "fill_now_correct": round(float((fill.price_next >= fill.price).mean()), 3) if len(fill) else None,
            "fill_now_mean_avoided_rise_ct": round(float(((fill.price_next - fill.price) * 100).mean()), 2) if len(fill) else None,
            "base_rate_price_falls_next_day": round(float((moved < 0).mean()), 3),
        },
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--test-weeks", type=int, default=8)
    p.add_argument("--fuels", default="diesel,e10,e5")
    p.add_argument("--out", default="results/forecast_eval.json")
    a = p.parse_args()
    bt = backtest(features(load(a.db, tuple(a.fuels.split(",")))), a.test_weeks)
    res = summarise(bt)
    res["settings"] = {"test_weeks": a.test_weeks, "fuels": a.fuels, "grain": "day (price at 12:00)"}
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=1))
    bt[["series", "day", "price", "price_next", "p10", "p50", "p90", "naive", "decision"]].to_parquet(
        out.with_suffix(".backtest.parquet"))
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()

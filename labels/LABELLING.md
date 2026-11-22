# Hand-labelling 100 real flags (portfolio plan P4)

`flag_labels_sheet.csv` holds 100 price changes, drawn at random (fixed seed) from the 7,050 that the gate flagged on real French data. The gate kept them but did not trust them. The question for each one: **is this price wrong, or is it a real price?**

The reason the gate gave is not in the sheet, on purpose, so it cannot steer you. It is in `.key/flag_labels_key.csv`. Do not open that file until all 100 are labelled.

## What to write in the `label` column

| Label | Use it when |
|---|---|
| `error` | The price cannot be right: a typo, a glitch, a price that jumps and comes straight back, far outside anything nearby |
| `real` | A genuine price, even if high or low: a motorway or remote station, a station that is simply expensive, a real price war |
| `unsure` | You cannot tell from what you see |

Use `note` for a few words on why, especially for `unsure`.

## How to judge

- Compare `price_eur` with `previous_price_eur`, `next_price_eur` and `france_median_that_day_eur`.
- A price that holds for days (previous and next are the same) is usually real. A one-off that reverts within the hour is usually an error.
- `station_id` starts with the station's postcode. Paris (75…), motorway and island stations are often expensive for real. You can look a station up on prix-carburants.gouv.fr.
- Do not look things up in the gate code or the key. Judge each row as a person would.

## When you are done

Save the file as it is (CSV) and tell Claude. Precision on real flags = `error` / (`error` + `real`), reported with the count of `unsure` beside it. It is published whatever it comes out as.

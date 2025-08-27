# AU Lipstick Index — Scaffold

This folder contains a ready-to-run scaffold to compute an Australia-specific **Lipstick Index** and flag months where **small-luxury** spending rises while the **ASX 200** falls.

## What you’ll need to drop in
1. **ABS Retail Trade (Monthly, SA, $m)**:
   - *Clothing, footwear & personal accessory retailing* (industry **group**, national total)
   - *Pharmaceutical, cosmetic & toiletry goods retailing* (industry **subgroup**, sum of states to national)
   - Save each series as CSV using the templates in `data/templates/`

   Tip: From the ABS “Retail Trade, Australia — latest release”, use **Download table as CSV** next to:
   - **Clothing, footwear & personal accessory retailing** (group) — or download **Table 1** (by industry group) and export that column.
   - **Other retailing → by subgroup** (use **Table 12** “state by industry subgroup, seasonally adjusted” and sum across states for *Pharmaceutical, cosmetic & toiletry goods retailing*).

2. **ASX 200 end‑of‑month levels** (official, from ASX “Historical market statistics”). Copy/paste into `data/asx200_eom.csv` using the provided template.

## How to run
```bash
# Option A: run the Python script
python lipstick_index.py

# Option B: run from a notebook and import the functions
```

The script will:
- Read cosmetics + clothing series and compute MoM % changes (seasonally adjusted).
- Build a composite: 60% cosmetics + 40% clothing (weights can be changed).
- Compute ASX 200 monthly % returns from end-of-month levels.
- Output a CSV and a pretty table of months where **Composite > 0** and **ASX200 < 0**.
- Save two charts in `output/`:
  - `lipstick_vs_asx_timeseries.png`
  - `lipstick_scatter.png`


## One-command auto-fetch
Run `python auto_fetch_and_run.py` to download the ABS Table 1 & 12 spreadsheets and the ASX200 EOM list, build the index, and output divergences plus charts.

#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

BASE = os.path.dirname(__file__)
DATA = os.path.join(BASE, "data")
OUT  = os.path.join(BASE, "output")
os.makedirs(OUT, exist_ok=True)

# ===== Parameters =====
WEIGHT_COSMETICS = 0.60
WEIGHT_CLOTHING  = 0.40

def _read_series_csv(path, value_col):
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"{os.path.basename(path)} is empty. Please fill it using templates in data/templates/.")
    # parse
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    # coerce numeric
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    return df

def load_inputs():
    cosmetics = _read_series_csv(os.path.join(DATA, "cosmetics_sa.csv"), "value_aud_m_sa")
    clothing  = _read_series_csv(os.path.join(DATA, "clothing_sa.csv"),  "value_aud_m_sa")
    asx       = _read_series_csv(os.path.join(DATA, "asx200_eom.csv"),   "asx200_eom_index")
    return cosmetics, clothing, asx

def compute_mom_pct(df, valcol, newcol):
    df = df.copy()
    df[newcol] = df[valcol].pct_change() * 100.0
    return df

def build_index(cosmetics_mom, clothing_mom):
    # Align by date
    df = pd.merge(cosmetics_mom[['date','cosmetics_mom']],
                  clothing_mom[['date','clothing_mom']],
                  on='date', how='inner')
    df['lipstick_index'] = WEIGHT_COSMETICS*df['cosmetics_mom'] + WEIGHT_CLOTHING*df['clothing_mom']
    return df

def compute_asx_returns(asx_df):
    asx_df = asx_df.copy()
    asx_df['asx200_ret_mom'] = asx_df['asx200_eom_index'].pct_change() * 100.0
    return asx_df[['date','asx200_ret_mom']]

def find_divergences(li_df, asx_ret):
    merged = pd.merge(li_df, asx_ret, on='date', how='inner')
    cond = (merged['lipstick_index'] > 0.0) & (merged['asx200_ret_mom'] < 0.0)
    out = merged.loc[cond].copy()
    out = out[['date','cosmetics_mom','clothing_mom','lipstick_index','asx200_ret_mom']]
    out = out.sort_values('date').reset_index(drop=True)
    return merged, out

def save_charts(merged_df):
    # Time series
    plt.figure(figsize=(10,5))
    plt.plot(merged_df['date'], merged_df['lipstick_index'], label='Lipstick Index (MoM, %)')
    plt.plot(merged_df['date'], merged_df['asx200_ret_mom'], label='ASX200 Return (MoM, %)')
    plt.legend()
    plt.title("Lipstick Index vs ASX200 Monthly Returns")
    plt.xlabel("Date")
    plt.ylabel("%")
    plt.tight_layout()
    ts_path = os.path.join(OUT, "lipstick_vs_asx_timeseries.png")
    plt.savefig(ts_path, dpi=160)
    plt.close()

    # Scatter
    plt.figure(figsize=(6,6))
    plt.scatter(merged_df['asx200_ret_mom'], merged_df['lipstick_index'])
    plt.axhline(0)
    plt.axvline(0)
    plt.xlabel("ASX200 MoM return (%)")
    plt.ylabel("Lipstick Index MoM (%)")
    plt.title("Lipstick vs ASX200 (monthly)")
    plt.tight_layout()
    sc_path = os.path.join(OUT, "lipstick_scatter.png")
    plt.savefig(sc_path, dpi=160)
    plt.close()

    return ts_path, sc_path

def main():
    cosmetics, clothing, asx = load_inputs()

    cosmetics_m = compute_mom_pct(cosmetics, 'value_aud_m_sa', 'cosmetics_mom')
    clothing_m  = compute_mom_pct(clothing,  'value_aud_m_sa', 'clothing_mom')
    li_df = build_index(cosmetics_m, clothing_m)
    asx_ret = compute_asx_returns(asx)

    merged, divergences = find_divergences(li_df, asx_ret)

    # Save outputs
    out_csv = os.path.join(OUT, "lipstick_divergences.csv")
    divergences.to_csv(out_csv, index=False)

    ts_path, sc_path = save_charts(merged)

    # Pretty print
    if divergences.empty:
        print("No divergence months found (Lipstick > 0 & ASX200 < 0) for the overlapping period.")
    else:
        print("Divergence months (Lipstick up, ASX200 down):")
        print(divergences.to_string(index=False, formatters={
            'cosmetics_mom': '{:.2f}'.format,
            'clothing_mom': '{:.2f}'.format,
            'lipstick_index': '{:.2f}'.format,
            'asx200_ret_mom': '{:.2f}'.format,
        }))
    print(f"\nSaved CSV: {out_csv}")
    print(f"Saved charts: {ts_path}, {sc_path}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import time
import json
from typing import Dict, List, Optional

import pandas as pd


# -------------------------
# Poka-yoke / error handling
# -------------------------

def die(msg: str, code: int = 2) -> None:
    print(f"❌ {msg}", file=sys.stderr)
    raise SystemExit(code)

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def read_parquet_strict(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        die(f"Missing required file: {path}")
    try:
        return pd.read_parquet(path)
    except Exception as e:
        die(f"Failed to read parquet: {path}\nReason: {e}")

def find_qty_col(df: pd.DataFrame) -> Optional[str]:
    # Extra safety if some table outputs differ (just in case)
    candidates = ["Qty", "Qty, pcs", "QTY", "qty", "кол-во", "Количество"]
    for c in candidates:
        if c in df.columns:
            return c
    return None

def load_qty_series(source_name: str) -> pd.Series:
    """
    Loads /data/<source>/<source>.parquet and returns a Series indexed by SKU with numeric Qty.
    """
    p = os.path.join("data", source_name, f"{source_name}.parquet")
    df = read_parquet_strict(p)

    if "SKU" not in df.columns:
        die(f"{p}: missing 'SKU' column. Columns={list(df.columns)}")

    qty_col = find_qty_col(df)
    if not qty_col:
        die(f"{p}: missing Qty column (expected 'Qty' or similar). Columns={list(df.columns)}")

    out = df[["SKU", qty_col]].copy()
    out["SKU"] = out["SKU"].astype(str).str.strip()
    out[qty_col] = pd.to_numeric(out[qty_col], errors="coerce").fillna(0.0)

    # In case there are duplicates (shouldn't happen, but poka-yoke)
    s = out.groupby("SKU", as_index=True)[qty_col].sum()
    s.name = source_name
    return s


# -------------------------
# Main aggregation
# -------------------------

def main() -> None:
    t0 = time.perf_counter()
    print(f"🟢 {now_ts()} Start SKU_updated aggregation")

    # Primary table
    base_name = "SKU"
    base_path = os.path.join("data", base_name, f"{base_name}.parquet")
    sku_df = read_parquet_strict(base_path)

    required_base_cols = ["SKU", "Model", "RSP", "ETA_Almaty"]
    missing = [c for c in required_base_cols if c not in sku_df.columns]
    if missing:
        die(f"{base_path}: missing required columns: {missing}\nColumns={list(sku_df.columns)}")

    sku_df = sku_df[required_base_cols].copy()
    sku_df["SKU"] = sku_df["SKU"].astype(str).str.strip()
    sku_df["Model"] = sku_df["Model"].astype(str).str.strip()

    # Make SKU unique (primary key). If duplicates exist -> keep first non-null-ish, but warn.
    if sku_df["SKU"].duplicated().any():
        print("⚠️ Warning: SKU.parquet contains duplicate SKUs. Deduplicating by first occurrence.")
        sku_df = sku_df.drop_duplicates(subset=["SKU"], keep="first")

    sku_df = sku_df.set_index("SKU", drop=True)

    # Load secondary tables (as Series by SKU)
    # Names must match your /data/<name>/<name>.parquet folders
    s_in_transit         = load_qty_series("in_transit_table")
    s_transit_booked     = load_qty_series("transit_booked_table")
    s_preorders          = load_qty_series("preorders_table")

    s_inbound            = load_qty_series("inbound_table")
    s_sold_to_clients    = load_qty_series("sold_to_clients_table")
    s_sold_to_distr      = load_qty_series("sold_to_distr_table")
    s_tref               = load_qty_series("tref_table")
    s_demo               = load_qty_series("demo_table")
    s_demo_astana        = load_qty_series("demo_astana_table")
    s_service            = load_qty_series("service_table")
    s_booked             = load_qty_series("booked_table")
    s_spark              = load_qty_series("spark_table")

    # Align to primary SKU index (missing -> 0)
    def align(s: pd.Series) -> pd.Series:
        return s.reindex(sku_df.index).fillna(0.0)

    in_transit = align(s_in_transit) - align(s_transit_booked)
    total_preorders = align(s_preorders)

    stock = (
        align(s_inbound)
        - align(s_sold_to_clients)
        - align(s_sold_to_distr)
        - align(s_tref)
        - align(s_demo)
        - align(s_demo_astana)
        - align(s_service)
        - align(s_booked)
        - align(s_spark)
    )

    out = sku_df.copy()
    out["In_transit"] = in_transit
    out["Total_preorders"] = total_preorders
    out["Stock"] = stock

    # Column order exactly as you requested
    out = out.reset_index()
    out = out[["SKU", "Model", "In_transit", "Total_preorders", "Stock", "RSP", "ETA_Almaty"]]

    # Clean numeric types (int if whole, else float)
    for c in ["In_transit", "Total_preorders", "Stock", "RSP"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
        out[c] = out[c].apply(lambda x: int(x) if float(x).is_integer() else float(x))

    # Save outputs
    name = "SKU_updated"
    out_dir = os.path.join("data", name)
    ensure_dir(out_dir)

    out.to_parquet(os.path.join(out_dir, f"{name}.parquet"), index=False)

    # CSV: keep ETA as dd.mm.yyyy, blank if missing
    out_csv = out.copy()
    out_csv["ETA_Almaty"] = pd.to_datetime(out_csv["ETA_Almaty"], errors="coerce").dt.strftime("%d.%m.%Y")
    out_csv.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False, encoding="utf-8")

    # JSON: make NaT/NaN safe
    out_json = out.copy()
    out_json["ETA_Almaty"] = pd.to_datetime(out_json["ETA_Almaty"], errors="coerce").dt.strftime("%d.%m.%Y")
    out_json = out_json.where(pd.notna(out_json), None)
    with open(os.path.join(out_dir, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(out_json.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    total_s = round(time.perf_counter() - t0, 2)
    print(f"✅ Done. Rows={len(out)} | Runtime={total_s}s")
    print(f"📁 Outputs: /data/{name}/{name}.parquet|csv|json")


if __name__ == "__main__":
    main()

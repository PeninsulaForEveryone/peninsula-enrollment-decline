"""
fetch_housing.py
────────────────
Downloads and cleans housing production data for San Mateo County jurisdictions.

Two sources:
  1. HCD APR (2018–present): building permits by income category, jurisdiction-level
     Source: California Open Data Portal
  2. Census BPS (2010–2017): annual permit totals, used as pre-APR supplement
     Source: Census Bureau county-level building permits survey

Output: data/processed/housing_clean.parquet
        Columns: year (int), jurisdiction (str), county (str),
                 permits_total (int), permits_vl (int), permits_low (int),
                 permits_mod (int), permits_above_mod (int), source (str)

Run standalone:  python -m pipeline.fetch_housing
"""

import logging
import sys
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import (
    CENSUS_BPS_YEARS,
    DATA_PROCESSED,
    DATA_RAW,
    HCD_APR_CSV_URL,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

HOUSING_RAW_DIR = DATA_RAW / "housing"
HOUSING_RAW_DIR.mkdir(parents=True, exist_ok=True)

SMC_COUNTY = "San Mateo"
CA_FIPS_STATE = "06"
SMC_FIPS_COUNTY = "081"


# ── HCD APR ────────────────────────────────────────────────────────────────────

def _fetch_apr_raw(force: bool = False) -> pd.DataFrame:
    """
    Download the HCD APR CSV from California Open Data Portal.

    The dataset has one row per (jurisdiction × year × project type) at minimum.
    Table A2 is the permits-issued table (the RHNA tracking table).

    If the direct resource URL breaks, fall back to the CKAN package API
    to discover the current resource URL.
    """
    dest = HOUSING_RAW_DIR / "hcd_apr_raw.csv"

    if not dest.exists() or force:
        log.info("Downloading HCD APR data...")
        downloaded = False

        # Primary: direct resource download
        try:
            r = requests.get(HCD_APR_CSV_URL, timeout=120)
            r.raise_for_status()
            dest.write_bytes(r.content)
            downloaded = True
            log.info("  saved to %s (%d bytes)", dest.name, len(r.content))
        except Exception as e:
            log.warning("  primary URL failed: %s", e)

        # Fallback: discover resource via CKAN API
        if not downloaded:
            log.info("  trying CKAN package API fallback...")
            try:
                api_url = (
                    "https://data.ca.gov/api/3/action/package_show"
                    "?id=housing-element-annual-progress-report-apr-data-by-jurisdiction-and-year"
                )
                meta = requests.get(api_url, timeout=30).json()
                resources = meta["result"]["resources"]
                # Find a CSV resource (prefer Table A2 / permits)
                csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
                if csv_resources:
                    csv_url = csv_resources[0]["url"]
                    log.info("  found resource: %s", csv_url)
                    r = requests.get(csv_url, timeout=120)
                    r.raise_for_status()
                    dest.write_bytes(r.content)
                    downloaded = True
                    log.info("  saved fallback CSV (%d bytes)", len(r.content))
            except Exception as e:
                log.warning("  CKAN fallback failed: %s", e)

        if not downloaded:
            raise RuntimeError(
                "Could not download HCD APR data. Check:\n"
                "  https://data.ca.gov/dataset/"
                "housing-element-annual-progress-report-apr-data-by-jurisdiction-and-year\n"
                "and update HCD_APR_CSV_URL in config.py"
            )
    else:
        log.info("  cache hit: %s", dest.name)

    return pd.read_csv(dest, dtype=str, low_memory=False)


def _clean_apr(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and aggregate HCD APR data to (jurisdiction x year) totals.

    Confirmed column names from data.ca.gov dataset (2024):
      cnty_name, juris_name, year, application_status,
      vlow_income_dr, vlow_income_ndr,
      low_income_dr,  low_income_ndr,
      mod_income_dr,  mod_income_ndr,
      above_mod_income

    application_status values: Approved, Disapproved, Pending, Withdrawn
    We keep only "Approved" rows.

    Income columns split into deed-restricted (dr) and non-deed-restricted (ndr);
    we sum both for each income tier.
    """
    df.columns = [c.strip().lower() for c in df.columns]
    log.info("APR raw shape: %s, columns: %s", df.shape, list(df.columns[:10]))

    # Filter to San Mateo County
    df = df[df["cnty_name"].str.strip().str.lower() == "san mateo"].copy()
    log.info("  SMC rows: %d", len(df))

    # Parse year
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[df["year"].notna() & (df["year"] >= 2018)].copy()
    df["year"] = df["year"].astype(int)

    # Jurisdiction
    df["jurisdiction"] = df["juris_name"].str.strip()

    # Keep approved permits only
    df = df[df["application_status"].str.strip().str.lower() == "approved"].copy()
    log.info("  SMC approved rows: %d", len(df))

    # Income-category unit counts (deed-restricted + non-deed-restricted)
    def _to_num(col):
        return pd.to_numeric(df[col], errors="coerce").fillna(0) if col in df.columns else 0

    df["permits_vl"]        = _to_num("vlow_income_dr") + _to_num("vlow_income_ndr")
    df["permits_low"]       = _to_num("low_income_dr")  + _to_num("low_income_ndr")
    df["permits_mod"]       = _to_num("mod_income_dr")  + _to_num("mod_income_ndr")
    df["permits_above_mod"] = _to_num("above_mod_income")
    df["permits_total"]     = df[["permits_vl", "permits_low", "permits_mod", "permits_above_mod"]].sum(axis=1)

    # Aggregate to jurisdiction x year
    agg = (
        df.groupby(["jurisdiction", "year"])
        .agg(
            permits_total    =("permits_total",     "sum"),
            permits_vl       =("permits_vl",        "sum"),
            permits_low      =("permits_low",        "sum"),
            permits_mod      =("permits_mod",        "sum"),
            permits_above_mod=("permits_above_mod",  "sum"),
        )
        .reset_index()
    )
    agg["county"] = SMC_COUNTY
    agg["source"] = "HCD_APR"
    log.info(
        "  APR clean: %d jurisdiction-years, %d total approved units",
        len(agg), int(agg["permits_total"].sum())
    )
    return agg


def _find_col(df: pd.DataFrame, fragments: list) -> Optional[str]:
    """Return first column name that contains any of the given substrings (case-insensitive)."""
    for frag in fragments:
        for col in df.columns:
            if frag.lower() in col.lower():
                return col
    return None


# ── Census Building Permits Survey (pre-2018 supplement) ──────────────────────

def _fetch_census_bps(year: int, force: bool = False) -> Optional[pd.DataFrame]:
    """
    Fetch Census BPS county annual file for a given year.

    Format: fixed-width or comma-delimited depending on year.
    We want San Mateo County (state=06, county=081), total units permitted.

    Census BPS county annual files: https://www2.census.gov/econ/bps/County/
    File name pattern: co{year}a.txt (annual totals)
    Layout: state_fips, county_fips, msa_fips, ..., 1unit_permits, ...
    """
    url = f"https://www2.census.gov/econ/bps/County/co{year}a.txt"
    dest = HOUSING_RAW_DIR / f"bps_{year}.txt"

    if not dest.exists() or force:
        log.info("  downloading Census BPS %d", year)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            dest.write_bytes(r.content)
        except Exception as e:
            log.warning("  Census BPS %d failed: %s", year, e)
            return None
    else:
        log.info("  cache hit: bps_%d.txt", year)

    raw = dest.read_text(encoding="latin-1")

    # BPS county annual file format (confirmed from 2015 file):
    #   Row 0: header line 1  (Survey, FIPS, FIPS, Region, Division, County, ...)
    #   Row 1: header line 2  (Date, State, County, Code, Code, Name, Bldgs, Units, Value, ...)
    #   Row 2: blank
    #   Row 3+: data  →  year, state_fips(2), county_fips(3), region, division, name,
    #                     1unit_bldgs, 1unit_units, 1unit_val,
    #                     2unit_bldgs, 2unit_units, 2unit_val,
    #                     34unit_bldgs, 34unit_units, 34unit_val,
    #                     5plus_bldgs, 5plus_units, 5plus_val, ...
    #   Units permitted = columns 7 + 10 + 13 + 16 (0-indexed)
    try:
        df = pd.read_csv(
            StringIO(raw), dtype=str, header=None,
            skiprows=3, low_memory=False, on_bad_lines="skip",
        )
    except Exception as e:
        log.warning("  Could not parse BPS %d: %s", year, e)
        return None

    # Filter to San Mateo County: col[1]=="06", col[2]=="081"
    try:
        mask = (
            df.iloc[:, 1].str.strip().str.zfill(2) == CA_FIPS_STATE
        ) & (
            df.iloc[:, 2].str.strip().str.zfill(3) == SMC_FIPS_COUNTY
        )
        smc_rows = df[mask]
    except Exception as e:
        log.warning("  FIPS filter failed for BPS %d: %s", year, e)
        return None

    if smc_rows is None or smc_rows.empty:
        log.warning("  SMC not found in BPS %d", year)
        return None

    # Sum units columns: 1-unit(7), 2-unit(10), 3-4unit(13), 5+unit(16)
    unit_cols = [7, 10, 13, 16]
    available = [c for c in unit_cols if c < len(smc_rows.columns)]
    total_permits = int(
        smc_rows.iloc[:, available]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .values.sum()
    )

    # BPS doesn't have income-category breakdown; use -1 as sentinel
    return pd.DataFrame([{
        "jurisdiction": "San Mateo County (aggregate)",
        "year": year,
        "county": SMC_COUNTY,
        "permits_total": total_permits,
        "permits_vl": 0,
        "permits_low": 0,
        "permits_mod": 0,
        "permits_above_mod": total_permits,  # BPS has no income split
        "source": "Census_BPS",
    }])


def fetch_census_bps(force: bool = False) -> pd.DataFrame:
    """Download all Census BPS supplement years and return combined df."""
    frames = []
    for year in CENSUS_BPS_YEARS:
        df = _fetch_census_bps(year, force=force)
        if df is not None:
            frames.append(df)
    if not frames:
        log.warning("No Census BPS data fetched.")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ── Main ───────────────────────────────────────────────────────────────────────

def run(force: bool = False) -> pd.DataFrame:
    """
    Full housing pipeline. Returns clean jurisdiction-level dataframe.

    Columns: year, jurisdiction, county, permits_total, permits_vl,
             permits_low, permits_mod, permits_above_mod, source
    """
    log.info("=== Fetching HCD APR data ===")
    apr_raw = _fetch_apr_raw(force=force)
    apr_clean = _clean_apr(apr_raw)
    log.info("APR clean: %d rows, years %s", len(apr_clean), sorted(apr_clean["year"].unique()))

    log.info("=== Fetching Census BPS supplement (2010–2017) ===")
    bps = fetch_census_bps(force=force)
    if not bps.empty:
        log.info("BPS: %d rows", len(bps))

    combined = pd.concat([apr_clean, bps], ignore_index=True) if not bps.empty else apr_clean
    combined = combined.sort_values(["jurisdiction", "year"]).reset_index(drop=True)

    # Cumulative permits per jurisdiction (useful for scatter plot)
    combined["permits_cumulative"] = combined.groupby("jurisdiction")["permits_total"].cumsum()

    out_path = DATA_PROCESSED / "housing_clean.parquet"
    combined.to_parquet(out_path, index=False)
    log.info("Saved %d rows → %s", len(combined), out_path)
    combined.to_csv(DATA_PROCESSED / "housing_clean.csv", index=False)
    return combined


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch and clean HCD APR + Census BPS data")
    parser.add_argument("--force", action="store_true", help="Re-download cached files")
    args = parser.parse_args()
    run(force=args.force)

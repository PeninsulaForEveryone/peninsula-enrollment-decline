"""
Central configuration for enrollment-vs-housing pipeline.

All magic numbers, URL templates, and field mappings live here so
the individual fetch/merge scripts stay readable.
"""

from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
DOCS = ROOT / "docs"

# ── CDE: San Mateo County ──────────────────────────────────────────────────────
SMC_COUNTY_CODE = "41"          # 2-digit FIPS used in CDE files
SMC_COUNTY_CODE_5 = "41000"     # 5-digit county+district prefix for county-office rows

# ── CDE enrollment bundle files ────────────────────────────────────────────────
# CDE consolidated historical enrollment into multi-year bundles at www3.cde.ca.gov.
# Source page: https://www.cde.ca.gov/ds/ad/fileshistenr8122.asp
#
# Each bundle covers multiple academic years in a single file.
# File structure (2014-2022): https://www.cde.ca.gov/ds/ad/fsenrps.asp
#   Columns: CDS_CODE (14-char), ACADEMIC_YEAR, ETHNIC, GENDER,
#            KDGN, GR_1..GR_12, UNGR_ELM, UNGR_SEC, ENR_TOTAL, ADULT
#
# Bundle files (filename, url, academic years as "YYYY-YY" strings):
# Each bundle covers school years STARTING in those calendar years:
#   enr201416 → starting 2014, 2015, 2016 → "2014-15", "2015-16", "2016-17"
#   enr201719 → starting 2017, 2018, 2019 → "2017-18", "2018-19", "2019-20"
#   enr202022 → starting 2020, 2021, 2022 → "2020-21", "2021-22", "2022-23"
CDE_BUNDLE_FILES = [
    (
        "enr201416-v2.txt",
        "https://www3.cde.ca.gov/demo-downloads/enrsch/enr201416-v2.txt",
        ["2014-15", "2015-16", "2016-17"],
    ),
    (
        "enr201719-v2.txt",
        "https://www3.cde.ca.gov/demo-downloads/enrsch/enr201719-v2.txt",
        ["2017-18", "2018-19", "2019-20"],
    ),
    (
        "enr202022-v2.txt",
        "https://www3.cde.ca.gov/demo-downloads/enrsch/enr202022-v2.txt",
        ["2020-21", "2021-22", "2022-23"],
    ),
]

# Column layout for 2014–2022 bundle files (tab-delimited, has header row).
# Grade columns to sum for K-12 total (excluding ADULT):
GRADE_COLS_BUNDLE = (
    ["KDGN"] + [f"GR_{i}" for i in range(1, 13)] + ["UNGR_ELM", "UNGR_SEC"]
)

# 2023-24+: new Census Day format (TK as real grade).
# Verified from https://www.cde.ca.gov/ds/ad/filesenrcensus.asp
# All files at: https://www3.cde.ca.gov/demo-downloads/census/
CDE_CENSUS_FILES = [
    (2024, "https://www3.cde.ca.gov/demo-downloads/census/cdenroll2324-v2.txt"),
    (2025, "https://www3.cde.ca.gov/demo-downloads/census/cdenroll2425.txt"),
    (2026, "https://www3.cde.ca.gov/demo-downloads/census/cdenroll2526.txt"),
]
# Aliases used by fetch_enrollment.py
CDE_NEW_URL   = CDE_CENSUS_FILES[0][1]
CDE_NEW_YEARS = [year for year, _ in CDE_CENSUS_FILES]

# New Census Day column mappings (lowercase normalised names):
NEW_KEY_COLS = {
    "AcademicYear":   "academic_year",
    "AggregateLevel": "agg_level",
    "CountyCode":     "county_code",
    "DistrictCode":   "district_code",
    "SchoolCode":     "school_code",
    "DistrictName":   "district_name",
    "Enrollment":     "enrollment",
}

# ── New Census Day column layout (2023-24+) ────────────────────────────────────
# The new file has different columns; TK is its own grade.
# Key fields we care about:
NEW_KEY_COLS = {
    "AcademicYear":     "academic_year",
    "AggregateLevel":   "agg_level",      # D = district, S = school, C = county, T = state
    "CountyCode":       "county_code",
    "DistrictCode":     "district_code",
    "SchoolCode":       "school_code",
    "DistrictName":     "district_name",
    "Enrollment":       "enrollment",     # already summed across grades in district-level rows
}

# ── HCD APR ────────────────────────────────────────────────────────────────────
# California Open Data Portal — CKAN resource.
# Dataset slug: housing-element-annual-progress-report-apr-data-by-jurisdiction-and-year
# Direct CSV export via CKAN datastore API (no auth needed, public dataset).
# Resource ID verified 2024 — check data.ca.gov if the URL breaks.
HCD_APR_CSV_URL = (
    "https://data.ca.gov/dataset/"
    "housing-element-annual-progress-report-apr-data-by-jurisdiction-and-year"
    "/resource/9ce012e2-5fd3-4372-a4dd-d8f786b5a69e/download/"
    "table_a2_annual_building_permits_issued.csv"
)

# Fallback: full dataset export (larger, ~50MB, all tables)
HCD_APR_FULL_CSV_URL = (
    "https://data.ca.gov/api/3/action/datastore_search_sql"
    "?sql=SELECT%20*%20FROM%20%229ce012e2-5fd3-4372-a4dd-d8f786b5a69e%22"
    "%20WHERE%20%22County%22%20%3D%20%27San%20Mateo%27"
)

# Census Building Permits Survey (pre-2018 housing supplement)
# Monthly release from Census Bureau; we'll pull annual totals.
# Format: state=06 (CA), county=081 (San Mateo)
CENSUS_BPS_URL = (
    "https://www2.census.gov/econ/bps/County/"
    "co{year}a.txt"          # annual files; year = 4-digit
)
CENSUS_BPS_YEARS = list(range(2010, 2018))   # pre-HCD-APR supplement

# ── SMC jurisdiction → school district crosswalk ──────────────────────────────
# Hand-built. Each HCD jurisdiction maps to the ELEMENTARY district(s) it
# primarily feeds. High school districts span multiple cities so they're
# listed separately. CDS codes: 41-XXXXX-0000000 (county-district-school).
#
# Source for boundaries: https://www.smcoe.org/schools/school-districts
# and https://www.cde.ca.gov/SchoolDirectory/
#
# Keys must exactly match juris_name values in HCD APR data (UPPERCASE).
# District codes are 5-digit strings matching district_code in enrollment data
# (positions 2-6 of the 14-char CDS code, extracted in fetch_enrollment.py).
JURISDICTION_DISTRICT_MAP = {
    "ATHERTON":            ["68957"],           # Las Lomitas Elementary
    "BELMONT":             ["68866"],           # Belmont-Redwood Shores Elementary
    "BRISBANE":            ["68874"],           # Brisbane Elementary
    "BURLINGAME":          ["68882"],           # Burlingame Elementary
    "COLMA":               ["68916"],           # Jefferson Elementary
    "DALY CITY":           ["68916"],           # Jefferson Elementary
    "EAST PALO ALTO":      ["68999"],           # Ravenswood City Elementary
    "FOSTER CITY":         ["69039"],           # San Mateo-Foster City
    "HALF MOON BAY":       ["68890"],           # Cabrillo Unified
    "HILLSBOROUGH":        ["68908"],           # Hillsborough City Elementary
    "MENLO PARK":          ["68965"],           # Menlo Park City Elementary
    "MILLBRAE":            ["68973"],           # Millbrae Elementary
    "PACIFICA":            ["68932"],           # Pacifica SD
    "PORTOLA VALLEY":      ["68981"],           # Portola Valley Elementary
    "REDWOOD CITY":        ["68866", "69005"],  # Belmont-Redwood Shores + Redwood City Elementary
    "SAN BRUNO":           ["69013"],           # San Bruno Park Elementary
    "SAN CARLOS":          ["69021"],           # San Carlos Elementary
    "SAN MATEO":           ["69039"],           # San Mateo-Foster City
    "SAN MATEO COUNTY":    [],                  # unincorporated — no direct district match
    "SOUTH SAN FRANCISCO": ["69070"],           # South San Francisco Unified
}

# High school districts (Peninsula-spanning; shown separately in viz)
HS_DISTRICTS = {
    "Jefferson Union HSD":              "4165953",
    "San Mateo Union HSD":              "4169094",
    "Sequoia Union HSD":                "4169334",
    "South San Francisco USD":          "4169526",
    "Cabrillo USD":                     "4162215",
}

# ── Visualization output ───────────────────────────────────────────────────────
VIZ_OUTPUT_PATH = DATA_PROCESSED / "viz_data.json"

# Index year for enrollment normalization (enrollment[year] / enrollment[INDEX_YEAR] * 100)
INDEX_YEAR = 2018  # aligns with start of HCD APR data

# Minimum enrollment to include a district (filters tiny/special districts)
MIN_ENROLLMENT = 500

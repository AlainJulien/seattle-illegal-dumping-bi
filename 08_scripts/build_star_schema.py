import argparse
from pathlib import Path
import pandas as pd
import numpy as np

# -------------------------
# Helpers
# -------------------------

def std_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )

def std_upper_key(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.upper()
         .str.strip()
         .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )

def build_location_key(location: pd.Series, lat: pd.Series, lon: pd.Series) -> pd.Series:
    loc_std = (
        location.astype("string")
                .str.upper()
                .str.replace(r"[^A-Z0-9 ]+", "", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )

    lat_num = pd.to_numeric(lat, errors="coerce")
    lon_num = pd.to_numeric(lon, errors="coerce")

    # Treat 0,0 as missing (prevents fake hotspots)
    mask_00 = (lat_num == 0) & (lon_num == 0)
    lat_num = lat_num.mask(mask_00, np.nan)
    lon_num = lon_num.mask(mask_00, np.nan)

    lat_round = lat_num.round(4)
    lon_round = lon_num.round(4)

    has_geo = lat_round.notna() & lon_round.notna()
    geo_key = pd.Series(pd.NA, index=location.index, dtype="string")
    geo_key.loc[has_geo] = lat_round.loc[has_geo].astype("string") + "," + lon_round.loc[has_geo].astype("string")

    return geo_key.fillna(loc_std)

def data_quality_report(df: pd.DataFrame) -> dict:
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "rows_with_nulls_pct": round(df.isna().any(axis=1).mean() * 100, 2),
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_vals_pct_top5": (df.isna().mean() * 100).round(2).sort_values(ascending=False).head(5).to_dict()
    }

# -------------------------
# Cleaning
# -------------------------

def load_and_clean(input_path: Path) -> pd.DataFrame:
    df = pd.read_csv(input_path, low_memory=False)

    # Parse dates
    df["Created Date"] = pd.to_datetime(df.get("Created Date"), errors="coerce")

    # ZIP: keep 5-digit string or null
    if "ZIP Code" in df.columns:
        df["ZIP Code"] = df["ZIP Code"].astype("string").str.extract(r"(\d{5})", expand=False)

    # Council District: nullable int
    if "Council District" in df.columns:
        df["Council District"] = pd.to_numeric(df["Council District"], errors="coerce").astype("Int64")

    # Lat/Long: numeric, keep nulls; treat 0/0 as missing
    if "Latitude" in df.columns:
        df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    if "Longitude" in df.columns:
        df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    if "Latitude" in df.columns and "Longitude" in df.columns:
        mask_00 = (df["Latitude"] == 0) & (df["Longitude"] == 0)
        df.loc[mask_00, ["Latitude", "Longitude"]] = np.nan

    # Standardize string columns
    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = std_text(df[col])

    # Drop noisy columns if present
    df = df.drop(columns=["Community Reporting Area"], errors="ignore")

    # Safe categorical fills (slicing-friendly)
    safe_unknown_cols = [
        "Police Precinct",
        "Status",
        "Method Received",
        "Where is the Illegal Dumping Violation located?",
        "Choose a description of the Illegal Dumping",
        "Location",
    ]
    for col in safe_unknown_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Optional date parts (nice for profiling; dims handle time intelligence)
    df["Year"] = df["Created Date"].dt.year
    df["Month"] = df["Created Date"].dt.month
    df["Day"] = df["Created Date"].dt.day
    df["Weekday"] = df["Created Date"].dt.day_name()
    df["Hour"] = df["Created Date"].dt.hour

    return df

# -------------------------
# Star schema builder
# -------------------------

def build_star_schema(clean_df: pd.DataFrame):
    model_df = clean_df.copy()

    # Stable LocationKey
    model_df["LocationKey"] = build_location_key(
        model_df.get("Location", pd.Series(pd.NA, index=model_df.index)),
        model_df.get("Latitude", pd.Series(pd.NA, index=model_df.index)),
        model_df.get("Longitude", pd.Series(pd.NA, index=model_df.index)),
    )

    # Core fields (normalized)
    created_dt = pd.to_datetime(model_df.get("Created Date"), errors="coerce")
    method_received = std_text(model_df.get("Method Received", pd.Series(pd.NA, index=model_df.index)))
    status = std_text(model_df.get("Status", pd.Series(pd.NA, index=model_df.index)))
    precinct = std_text(model_df.get("Police Precinct", pd.Series(pd.NA, index=model_df.index)))

    violation_located = std_text(model_df.get("Where is the Illegal Dumping Violation located?", pd.Series(pd.NA, index=model_df.index)))
    dumping_desc = std_text(model_df.get("Choose a description of the Illegal Dumping", pd.Series(pd.NA, index=model_df.index)))

    # Fact (1 row per request)
    fact = pd.DataFrame({
        "ServiceRequestNumber": std_text(model_df.get("Service Request Number", pd.Series(pd.NA, index=model_df.index))),
        "CreatedDateTime": created_dt,
        # Power BI-friendly date key: midnight datetime
        "CreatedDate": created_dt.dt.normalize(),
        "MethodReceived": method_received,
        "Status": status,
        "PolicePrecinct": precinct,
        "CouncilDistrict": model_df.get("Council District"),
        "ZIPCode": model_df.get("ZIP Code"),
        "ViolationLocatedAt": violation_located,
        "DumpingDescription": dumping_desc,
        "LocationKey": model_df["LocationKey"],
    })

    fact["CategoryKey"] = (
        std_upper_key(fact["ViolationLocatedAt"]) + "|" +
        std_upper_key(fact["DumpingDescription"])
    )

    # DimDate
    dim_date = (
        pd.DataFrame({"Date": fact["CreatedDate"].dropna().drop_duplicates()})
          .sort_values("Date")
          .reset_index(drop=True)
    )
    dt = pd.to_datetime(dim_date["Date"])
    dim_date["Year"] = dt.dt.year
    dim_date["MonthNumber"] = dt.dt.month
    dim_date["MonthName"] = dt.dt.month_name()
    dim_date["DayOfWeekNumber"] = dt.dt.dayofweek
    dim_date["DayOfWeekName"] = dt.dt.day_name()
    dim_date["WeekOfYear"] = dt.dt.isocalendar().week.astype("Int64")

    # DimLocation
    dim_location = (
        model_df[["LocationKey", "Location", "Latitude", "Longitude", "ZIP Code", "Police Precinct", "Council District"]]
        .rename(columns={
            "ZIP Code": "ZIPCode",
            "Police Precinct": "PolicePrecinct",
            "Council District": "CouncilDistrict",
        })
        .copy()
    )
    dim_location["Location"] = std_text(dim_location["Location"])
    dim_location["PolicePrecinct"] = std_text(dim_location["PolicePrecinct"])
    dim_location = dim_location.drop_duplicates("LocationKey").reset_index(drop=True)

    # DimCategory
    dim_category = (
        pd.DataFrame({
            "ViolationLocatedAt": violation_located,
            "DumpingDescription": dumping_desc
        })
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_category["CategoryKey"] = (
        std_upper_key(dim_category["ViolationLocatedAt"]) + "|" +
        std_upper_key(dim_category["DumpingDescription"])
    )

    # DimIntake + DimStatus
    dim_intake = pd.DataFrame({"MethodReceived": fact["MethodReceived"].dropna().drop_duplicates()}).sort_values("MethodReceived")
    dim_status = pd.DataFrame({"Status": fact["Status"].dropna().drop_duplicates()}).sort_values("Status")

    return fact, dim_date, dim_location, dim_category, dim_intake, dim_status

# -------------------------
# QA Gates + Export
# -------------------------

def qa_and_export(fact, dim_date, dim_location, dim_category, dim_intake, dim_status, export_dir: Path):
    export_dir.mkdir(parents=True, exist_ok=True)

    # QA: fact grain
    dup_sr = fact["ServiceRequestNumber"].duplicated().sum()
    if dup_sr > 0:
        raise ValueError(f"Fact grain broken: {dup_sr} duplicate ServiceRequestNumber values found.")

    # QA: required join keys
    for key_col in ["LocationKey", "CategoryKey"]:
        nulls = fact[key_col].isna().sum()
        if nulls > 0:
            raise ValueError(f"Null join keys found in fact: {key_col} has {nulls} nulls.")

    # QA: dim key uniqueness
    if dim_location["LocationKey"].duplicated().any():
        raise ValueError("dim_location has duplicate LocationKey values.")
    if dim_category["CategoryKey"].duplicated().any():
        raise ValueError("dim_category has duplicate CategoryKey values.")

    # Export
    fact.to_csv(export_dir / "fact_illegal_dumping.csv", index=False)
    dim_date.to_csv(export_dir / "dim_date.csv", index=False)
    dim_location.to_csv(export_dir / "dim_location.csv", index=False)
    dim_category.to_csv(export_dir / "dim_category.csv", index=False)
    dim_intake.to_csv(export_dir / "dim_intake.csv", index=False)
    dim_status.to_csv(export_dir / "dim_status.csv", index=False)

    print("BUILD OK")
    print(f"exports: {export_dir.resolve()}")
    print(f"fact rows: {len(fact):,}")
    print(f"dim_date: {len(dim_date):,} | dim_location: {len(dim_location):,} | dim_category: {len(dim_category):,}")
    print(f"dim_intake: {len(dim_intake):,} | dim_status: {len(dim_status):,}")

# -------------------------
# Main
# -------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw Illegal Dumping CSV")
    parser.add_argument("--export-dir", default="exports", help="Directory to write star schema CSVs")
    args = parser.parse_args()

    input_path = Path(args.input)
    export_dir = Path(args.export_dir)

    clean_df = load_and_clean(input_path)
    print("DATA QUALITY:", data_quality_report(clean_df))

    fact, dim_date, dim_location, dim_category, dim_intake, dim_status = build_star_schema(clean_df)
    qa_and_export(fact, dim_date, dim_location, dim_category, dim_intake, dim_status, export_dir)

if __name__ == "__main__":
    main()

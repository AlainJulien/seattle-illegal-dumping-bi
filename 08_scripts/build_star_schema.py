#!/usr/bin/env python3

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


# -------------------------
# Helpers
# -------------------------

def std_text(s: pd.Series) -> pd.Series:
    """Trim, normalize empties, keep as pandas string dtype."""
    s = s.astype("string").str.strip()
    s = s.replace({"None": pd.NA, "none": pd.NA, "nan": pd.NA, "NaN": pd.NA, "": pd.NA})
    return s

def std_upper_key(s: pd.Series) -> pd.Series:
    """Standardize text and create a robust uppercase key (safe for joins)."""
    s = std_text(s)
    s = (
        s.str.upper()
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
    )
    return s

def parse_created_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Created Date"] = pd.to_datetime(df.get("Created Date"), errors="coerce")
    return df

def clean_geo(df: pd.DataFrame) -> pd.DataFrame:
    """Clean lat/lon, treat 0,0 as missing."""
    df = df.copy()

    if "Latitude" in df.columns:
        df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    else:
        df["Latitude"] = np.nan

    if "Longitude" in df.columns:
        df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    else:
        df["Longitude"] = np.nan

    mask_00 = (df["Latitude"] == 0) & (df["Longitude"] == 0)
    df.loc[mask_00, ["Latitude", "Longitude"]] = np.nan

    # optional rounding for more stable location keys
    df["Latitude_r5"] = df["Latitude"].round(5)
    df["Longitude_r5"] = df["Longitude"].round(5)

    return df

def clean_zip(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "ZIP Code" in df.columns:
        df["ZIP Code"] = df["ZIP Code"].astype("string").str.extract(r"(\d{5})", expand=False)
    else:
        df["ZIP Code"] = pd.NA
    return df

def clean_int_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Council District"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

def data_quality_report(df: pd.DataFrame) -> dict:
    """Quick stats useful for debugging."""
    return {
        "rows": int(len(df)),
        "created_date_nulls": int(df["Created Date"].isna().sum()) if "Created Date" in df.columns else None,
        "lat_nulls": int(df["Latitude"].isna().sum()) if "Latitude" in df.columns else None,
        "lon_nulls": int(df["Longitude"].isna().sum()) if "Longitude" in df.columns else None,
        "distinct_status": int(df["Status"].nunique(dropna=True)) if "Status" in df.columns else None,
        "distinct_method_received": int(df["Method Received"].nunique(dropna=True)) if "Method Received" in df.columns else None,
    }


# -------------------------
# Load & Clean
# -------------------------

def load_and_clean(input_path: Path) -> pd.DataFrame:
    df = pd.read_csv(input_path, low_memory=False)

    # Parse / clean core fields
    df = parse_created_date(df)

    # Standardize key text columns
    for col in [
        "Service Request Number",
        "Method Received",
        "Status",
        "Location",
        "Police Precinct",
        "Community Reporting Area",
        "Where is the Illegal Dumping Violation located?",
        "Choose a description of the Illegal Dumping",
    ]:
        if col in df.columns:
            df[col] = std_text(df[col])

    df = clean_zip(df)
    df = clean_int_cols(df)
    df = clean_geo(df)

    return df


# -------------------------
# Build Star Schema
# -------------------------

def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    d = df[["Created Date"]].copy()
    d = d.dropna(subset=["Created Date"]).drop_duplicates().reset_index(drop=True)

    d["Date"] = d["Created Date"].dt.date.astype("string")
    d["Year"] = d["Created Date"].dt.year.astype("Int64")
    d["Quarter"] = d["Created Date"].dt.quarter.astype("Int64")
    d["Month"] = d["Created Date"].dt.month.astype("Int64")
    d["MonthName"] = d["Created Date"].dt.month_name()
    d["Day"] = d["Created Date"].dt.day.astype("Int64")
    d["DayOfWeek"] = d["Created Date"].dt.dayofweek.astype("Int64")
    d["DayName"] = d["Created Date"].dt.day_name()
    d["Hour"] = d["Created Date"].dt.hour.astype("Int64")

    # Stable DateID based on Date + Hour (since Created Date includes time)
    d = d.sort_values(["Created Date"]).reset_index(drop=True)
    d["DateID"] = (d.index + 1).astype("Int64")

    # Keep Created Date for mapping, but you can drop later if you want
    return d[[
        "DateID", "Created Date", "Date", "Year", "Quarter", "Month", "MonthName",
        "Day", "DayOfWeek", "DayName", "Hour"
    ]]

def build_dim_intake(df: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({"MethodReceived": df["Method Received"]})
    dim["MethodReceivedKey"] = std_upper_key(dim["MethodReceived"])
    dim = dim.drop_duplicates(subset=["MethodReceivedKey"]).sort_values("MethodReceivedKey").reset_index(drop=True)
    dim["IntakeID"] = (dim.index + 1).astype("Int64")
    return dim[["IntakeID", "MethodReceived", "MethodReceivedKey"]]

def build_dim_status(df: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({"Status": df["Status"]})
    dim["StatusKey"] = std_upper_key(dim["Status"])
    dim = dim.drop_duplicates(subset=["StatusKey"]).sort_values("StatusKey").reset_index(drop=True)
    dim["StatusID"] = (dim.index + 1).astype("Int64")
    return dim[["StatusID", "Status", "StatusKey"]]

def build_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({
        "ViolationLocatedAt": df.get("Where is the Illegal Dumping Violation located?", pd.Series([pd.NA]*len(df))),
        "DumpingDescription": df.get("Choose a description of the Illegal Dumping", pd.Series([pd.NA]*len(df))),
    })
    dim["ViolationKey"] = std_upper_key(dim["ViolationLocatedAt"])
    dim["DescriptionKey"] = std_upper_key(dim["DumpingDescription"])
    dim["CategoryKey"] = dim["ViolationKey"].fillna("UNK") + "|" + dim["DescriptionKey"].fillna("UNK")

    dim = dim.drop_duplicates(subset=["CategoryKey"]).sort_values(["ViolationKey", "DescriptionKey"]).reset_index(drop=True)
    dim["CategoryID"] = (dim.index + 1).astype("Int64")

    return dim[[
        "CategoryID", "CategoryKey",
        "ViolationLocatedAt", "DumpingDescription",
        "ViolationKey", "DescriptionKey"
    ]]

def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[[
        "Location", "ZIP Code", "Council District", "Police Precinct", "Community Reporting Area",
        "Latitude_r5", "Longitude_r5"
    ]].copy()

    for c in ["Location", "Police Precinct", "Community Reporting Area"]:
        if c in dim.columns:
            dim[c] = std_text(dim[c])

    dim["LocationKey"] = (
        std_upper_key(dim["Location"]).fillna("UNK") + "|" +
        dim["ZIP Code"].fillna("UNK") + "|" +
        dim["Council District"].astype("string").fillna("UNK") + "|" +
        std_upper_key(dim["Police Precinct"]).fillna("UNK") + "|" +
        std_upper_key(dim["Community Reporting Area"]).fillna("UNK") + "|" +
        dim["Latitude_r5"].astype("string").fillna("UNK") + "|" +
        dim["Longitude_r5"].astype("string").fillna("UNK")
    )

    dim = dim.drop_duplicates(subset=["LocationKey"]).sort_values("LocationKey").reset_index(drop=True)
    dim["LocationID"] = (dim.index + 1).astype("Int64")

    return dim[[
        "LocationID", "LocationKey",
        "Location", "ZIP Code", "Council District",
        "Police Precinct", "Community Reporting Area",
        "Latitude_r5", "Longitude_r5"
    ]]

def build_fact(df: pd.DataFrame,
               dim_date: pd.DataFrame,
               dim_location: pd.DataFrame,
               dim_category: pd.DataFrame,
               dim_intake: pd.DataFrame,
               dim_status: pd.DataFrame) -> pd.DataFrame:

    fact = df.copy()

    # Build keys used for mapping
    fact["MethodReceivedKey"] = std_upper_key(fact["Method Received"])
    fact["StatusKey"] = std_upper_key(fact["Status"])

    viol = fact.get("Where is the Illegal Dumping Violation located?", pd.Series([pd.NA]*len(fact)))
    desc = fact.get("Choose a description of the Illegal Dumping", pd.Series([pd.NA]*len(fact)))
    fact["CategoryKey"] = std_upper_key(viol).fillna("UNK") + "|" + std_upper_key(desc).fillna("UNK")

    fact["LocationKey"] = (
        std_upper_key(fact["Location"]).fillna("UNK") + "|" +
        fact["ZIP Code"].fillna("UNK") + "|" +
        fact["Council District"].astype("string").fillna("UNK") + "|" +
        std_upper_key(fact["Police Precinct"]).fillna("UNK") + "|" +
        std_upper_key(fact["Community Reporting Area"]).fillna("UNK") + "|" +
        fact["Latitude_r5"].astype("string").fillna("UNK") + "|" +
        fact["Longitude_r5"].astype("string").fillna("UNK")
    )

    # Map DateID from Created Date (exact timestamp)
    fact = fact.merge(
        dim_date[["Created Date", "DateID"]],
        on="Created Date",
        how="left",
        validate="m:1"
    )

    # Map dimension IDs
    fact = fact.merge(dim_intake[["MethodReceivedKey", "IntakeID"]], on="MethodReceivedKey", how="left", validate="m:1")
    fact = fact.merge(dim_status[["StatusKey", "StatusID"]], on="StatusKey", how="left", validate="m:1")
    fact = fact.merge(dim_category[["CategoryKey", "CategoryID"]], on="CategoryKey", how="left", validate="m:1")
    fact = fact.merge(dim_location[["LocationKey", "LocationID"]], on="LocationKey", how="left", validate="m:1")

    # Build the final fact table (keep useful degenerate attributes)
    fact_out = pd.DataFrame({
        "ServiceRequestNumber": std_text(fact.get("Service Request Number", pd.Series([pd.NA]*len(fact)))),
        "DateID": fact["DateID"].astype("Int64"),
        "LocationID": fact["LocationID"].astype("Int64"),
        "CategoryID": fact["CategoryID"].astype("Int64"),
        "IntakeID": fact["IntakeID"].astype("Int64"),
        "StatusID": fact["StatusID"].astype("Int64"),

        # Optional degenerate fields (handy for troubleshooting)
        "CreatedDate": fact["Created Date"],
        "X_Value": pd.to_numeric(fact.get("X_Value"), errors="coerce"),
        "Y_Value": pd.to_numeric(fact.get("Y_Value"), errors="coerce"),
        "Latitude": fact["Latitude"],
        "Longitude": fact["Longitude"],
    })

    return fact_out


# -------------------------
# QA & Export
# -------------------------

def qa_star_schema(fact: pd.DataFrame,
                   dim_date: pd.DataFrame,
                   dim_location: pd.DataFrame,
                   dim_category: pd.DataFrame,
                   dim_intake: pd.DataFrame,
                   dim_status: pd.DataFrame) -> None:

    # Primary key-ish checks
    if fact["ServiceRequestNumber"].isna().mean() > 0.25:
        # Not always present for all rows? Flag only if truly weird.
        print("WARN: Many ServiceRequestNumber values are null.")

    # FK checks
    for fk in ["DateID", "LocationID", "CategoryID", "IntakeID", "StatusID"]:
        nulls = int(fact[fk].isna().sum())
        if nulls:
            raise ValueError(f"FK mapping failed: {fk} has {nulls:,} null(s). Check standardization/joins.")

    # Uniqueness checks on dims
    assert dim_intake["IntakeID"].is_unique
    assert dim_status["StatusID"].is_unique
    assert dim_category["CategoryID"].is_unique
    assert dim_location["LocationID"].is_unique
    assert dim_date["DateID"].is_unique

def export_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def build_star_schema(df: pd.DataFrame):
    dim_date = build_dim_date(df)
    dim_location = build_dim_location(df)
    dim_category = build_dim_category(df)
    dim_intake = build_dim_intake(df)
    dim_status = build_dim_status(df)

    fact = build_fact(df, dim_date, dim_location, dim_category, dim_intake, dim_status)

    return fact, dim_date, dim_location, dim_category, dim_intake, dim_status

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to Illegal_Dumping_Reports CSV")
    parser.add_argument("--export-dir", default="exports", help="Directory to write star schema CSVs")
    args = parser.parse_args()

    input_path = Path(args.input)
    export_dir = Path(args.export_dir)

    df = load_and_clean(input_path)
    print("DATA QUALITY:", data_quality_report(df))

    fact, dim_date, dim_location, dim_category, dim_intake, dim_status = build_star_schema(df)

    qa_star_schema(fact, dim_date, dim_location, dim_category, dim_intake, dim_status)

    export_csv(fact, export_dir / "fact_illegal_dumping.csv")
    export_csv(dim_date, export_dir / "dim_date.csv")
    export_csv(dim_location, export_dir / "dim_location.csv")
    export_csv(dim_category, export_dir / "dim_category.csv")
    export_csv(dim_intake, export_dir / "dim_intake.csv")
    export_csv(dim_status, export_dir / "dim_status.csv")

    print("EXPORT DONE:")
    print(f"  fact:         {len(fact):,}")
    print(f"  dim_date:     {len(dim_date):,}")
    print(f"  dim_location: {len(dim_location):,}")
    print(f"  dim_category: {len(dim_category):,}")
    print(f"  dim_intake:   {len(dim_intake):,}")
    print(f"  dim_status:   {len(dim_status):,}")

if __name__ == "__main__":
    main()

"""
Seattle Illegal Dumping - Star Schema Builder
==============================================

This script transforms raw Seattle illegal dumping data into a dimensional model
(star schema) suitable for Power BI analysis.

Author: [Your Name]
Project: Seattle Illegal Dumping BI Analysis
Date: [Date]

Usage:
    python build_star_schema.py --input raw_data.csv --export-dir exports/

Outputs:
    - fact_illegal_dumping.csv (fact table with ~266K rows)
    - dim_date.csv (date dimension)
    - dim_location.csv (location dimension with ZIP, coordinates)
    - dim_category.csv (violation type + dump description combinations)
    - dim_intake.csv (method received dimension)
    - dim_status.csv (status dimension)

Architecture:
    Star schema with 1 fact table and 5 dimension tables, following
    Kimball dimensional modeling methodology.
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

# ============================================================================
# HELPER FUNCTIONS - Data Standardization
# ============================================================================

def std_text(s: pd.Series) -> pd.Series:
    """
    Standardize text fields: strip whitespace, convert to string, handle nulls.
    
    Args:
        s: Pandas Series containing text data
        
    Returns:
        Cleaned Series with standardized text values
    """
    return (
        s.astype("string")
         .str.strip()
         .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )


def std_upper_key(s: pd.Series) -> pd.Series:
    """
    Create standardized uppercase keys for joining/deduplication.
    
    Used for creating composite keys where case-insensitive matching is needed.
    
    Args:
        s: Pandas Series to convert to uppercase keys
        
    Returns:
        Uppercase, trimmed Series suitable for use as join keys
    """
    return (
        s.astype("string")
         .str.upper()
         .str.strip()
         .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )


def build_location_key(location: pd.Series, lat: pd.Series, lon: pd.Series) -> pd.Series:
    """
    Generate stable location keys using coordinates when available, address otherwise.
    
    Strategy:
        1. If valid lat/long exists, use rounded coordinates (4 decimals ≈ 10m precision)
        2. Otherwise, fall back to normalized address text
        3. Treats (0, 0) as invalid coordinates (prevents false hotspots)
    
    This ensures multiple reports at the same physical location get the same key,
    enabling accurate location-based aggregation in Power BI.
    
    Args:
        location: Address strings
        lat: Latitude values
        lon: Longitude values
        
    Returns:
        Series of location keys (format: "lat,lon" or normalized address)
    """
    # Normalize address text (fallback option)
    loc_std = (
        location.astype("string")
                .str.upper()
                .str.replace(r"[^A-Z0-9 ]+", "", regex=True)  # Remove special chars
                .str.replace(r"\s+", " ", regex=True)         # Collapse whitespace
                .str.strip()
                .replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})
    )

    # Convert coordinates to numeric
    lat_num = pd.to_numeric(lat, errors="coerce")
    lon_num = pd.to_numeric(lon, errors="coerce")

    # Treat (0, 0) as missing - prevents fake hotspots at null island
    mask_00 = (lat_num == 0) & (lon_num == 0)
    lat_num = lat_num.mask(mask_00, np.nan)
    lon_num = lon_num.mask(mask_00, np.nan)

    # Round to 4 decimals (~10 meter precision)
    lat_round = lat_num.round(4)
    lon_round = lon_num.round(4)

    # Build coordinate-based keys where valid coordinates exist
    has_geo = lat_round.notna() & lon_round.notna()
    geo_key = pd.Series(pd.NA, index=location.index, dtype="string")
    geo_key.loc[has_geo] = (
        lat_round.loc[has_geo].astype("string") + "," + 
        lon_round.loc[has_geo].astype("string")
    )

    # Use coordinate key if available, otherwise normalized address
    return geo_key.fillna(loc_std)


def data_quality_report(df: pd.DataFrame) -> dict:
    """
    Generate data quality metrics for validation and documentation.
    
    Metrics include:
        - Total rows and columns
        - Percentage of rows with any null values
        - Count of duplicate rows
        - Top 5 columns by missing data percentage
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary of quality metrics
    """
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "rows_with_nulls_pct": round(df.isna().any(axis=1).mean() * 100, 2),
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_vals_pct_top5": (
            df.isna().mean() * 100
        ).round(2).sort_values(ascending=False).head(5).to_dict()
    }


# ============================================================================
# DATA CLEANING - ETL Transform Step
# ============================================================================

def load_and_clean(input_path: Path) -> pd.DataFrame:
    """
    Load raw CSV and apply cleaning transformations.
    
    Cleaning Steps:
        1. Parse dates with error handling
        2. Standardize ZIP codes to 5-digit strings
        3. Convert Council District to nullable integers
        4. Clean lat/long (treat 0,0 as missing)
        5. Standardize all text fields
        6. Drop noisy/low-value columns (Community Reporting Area)
        7. Fill categorical nulls with 'Unknown'
        8. Derive temporal features (Year, Month, Day, Weekday, Hour)
    
    Args:
        input_path: Path to raw Seattle illegal dumping CSV
        
    Returns:
        Cleaned DataFrame ready for dimensional modeling
    """
    # Load with low_memory=False to avoid dtype warnings on large files
    df = pd.read_csv(input_path, low_memory=False)

    # ----------------
    # Date Parsing
    # ----------------
    df["Created Date"] = pd.to_datetime(df.get("Created Date"), errors="coerce")

    # ----------------
    # Geographic Data
    # ----------------
    
    # ZIP: Extract 5-digit codes, keep as string (preserves leading zeros)
    if "ZIP Code" in df.columns:
        df["ZIP Code"] = (
            df["ZIP Code"]
              .astype("string")
              .str.extract(r"(\d{5})", expand=False)
        )

    # Council District: Nullable integer (allows proper aggregation + handles nulls)
    if "Council District" in df.columns:
        df["Council District"] = (
            pd.to_numeric(df["Council District"], errors="coerce")
              .astype("Int64")
        )

    # Lat/Long: Numeric with null handling
    if "Latitude" in df.columns:
        df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    if "Longitude" in df.columns:
        df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    # Treat (0, 0) as missing coordinates (common data quality issue)
    if "Latitude" in df.columns and "Longitude" in df.columns:
        mask_00 = (df["Latitude"] == 0) & (df["Longitude"] == 0)
        df.loc[mask_00, ["Latitude", "Longitude"]] = np.nan

    # ----------------
    # Text Standardization
    # ----------------
    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = std_text(df[col])

    # ----------------
    # Column Cleanup
    # ----------------
    
    # Drop columns with >75% missing data (not useful for analysis)
    df = df.drop(columns=["Community Reporting Area"], errors="ignore")

    # Fill categorical nulls with 'Unknown' (better than dropping rows)
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

    # ----------------
    # Feature Engineering
    # ----------------
    
    # Derive temporal dimensions (useful for validation, though Dim_Date handles time intelligence)
    df["Year"] = df["Created Date"].dt.year
    df["Month"] = df["Created Date"].dt.month
    df["Day"] = df["Created Date"].dt.day
    df["Weekday"] = df["Created Date"].dt.day_name()
    df["Hour"] = df["Created Date"].dt.hour

    return df


# ============================================================================
# STAR SCHEMA BUILDER - Dimensional Model Creation
# ============================================================================

def build_star_schema(clean_df: pd.DataFrame):
    """
    Transform cleaned flat file into star schema (1 fact + 5 dimensions).
    
    Dimensional Model:
        Fact_IllegalDumping: Transaction grain (1 row per service request)
        Dim_Date: Calendar dimension for time intelligence
        Dim_Location: Geographic hierarchy (Location → ZIP → Precinct → District)
        Dim_Category: Violation type × Dumping description combinations
        Dim_Intake: Method received (app, phone, web)
        Dim_Status: Request status (open, closed, etc.)
    
    Design Decisions:
        - LocationKey uses coordinates when available (stable across reports)
        - CategoryKey is composite (ViolationLocatedAt|DumpingDescription)
        - CreatedDate normalized to midnight (Power BI date relationship requirement)
        - All dimensions use natural keys (not surrogate IDs) for simplicity
    
    Args:
        clean_df: Cleaned DataFrame from load_and_clean()
        
    Returns:
        Tuple of (fact, dim_date, dim_location, dim_category, dim_intake, dim_status)
    """
    model_df = clean_df.copy()

    # ----------------
    # Generate Keys
    # ----------------
    
    # Stable location key (coordinate-based when possible)
    model_df["LocationKey"] = build_location_key(
        model_df.get("Location", pd.Series(pd.NA, index=model_df.index)),
        model_df.get("Latitude", pd.Series(pd.NA, index=model_df.index)),
        model_df.get("Longitude", pd.Series(pd.NA, index=model_df.index)),
    )

    # ----------------
    # Standardize Core Fields
    # ----------------
    
    created_dt = pd.to_datetime(model_df.get("Created Date"), errors="coerce")
    method_received = std_text(
        model_df.get("Method Received", pd.Series(pd.NA, index=model_df.index))
    )
    status = std_text(
        model_df.get("Status", pd.Series(pd.NA, index=model_df.index))
    )
    precinct = std_text(
        model_df.get("Police Precinct", pd.Series(pd.NA, index=model_df.index))
    )
    violation_located = std_text(
        model_df.get(
            "Where is the Illegal Dumping Violation located?", 
            pd.Series(pd.NA, index=model_df.index)
        )
    )
    dumping_desc = std_text(
        model_df.get(
            "Choose a description of the Illegal Dumping", 
            pd.Series(pd.NA, index=model_df.index)
        )
    )

    # ----------------
    # FACT TABLE
    # ----------------
    
    fact = pd.DataFrame({
        "ServiceRequestNumber": std_text(
            model_df.get("Service Request Number", pd.Series(pd.NA, index=model_df.index))
        ),
        "CreatedDateTime": created_dt,
        # Power BI requires date-only field for time intelligence relationships
        "CreatedDate": created_dt.dt.normalize(),  # Midnight datetime = date
        "MethodReceived": method_received,
        "Status": status,
        "PolicePrecinct": precinct,
        "CouncilDistrict": model_df.get("Council District"),
        "ZIPCode": model_df.get("ZIP Code"),
        "ViolationLocatedAt": violation_located,
        "DumpingDescription": dumping_desc,
        "LocationKey": model_df["LocationKey"],
    })

    # Composite category key (combines violation location + dump type)
    fact["CategoryKey"] = (
        std_upper_key(fact["ViolationLocatedAt"]) + "|" +
        std_upper_key(fact["DumpingDescription"])
    )

    # ----------------
    # DIM_DATE (Calendar Dimension)
    # ----------------
    
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

    # ----------------
    # DIM_LOCATION (Geographic Hierarchy)
    # ----------------
    
    dim_location = (
        model_df[[
            "LocationKey", "Location", "Latitude", "Longitude", 
            "ZIP Code", "Police Precinct", "Council District"
        ]]
        .rename(columns={
            "ZIP Code": "ZIPCode",
            "Police Precinct": "PolicePrecinct",
            "Council District": "CouncilDistrict",
        })
        .copy()
    )
    
    # Standardize text fields
    dim_location["Location"] = std_text(dim_location["Location"])
    dim_location["PolicePrecinct"] = std_text(dim_location["PolicePrecinct"])
    
    # Deduplicate on LocationKey (keep first occurrence)
    dim_location = dim_location.drop_duplicates("LocationKey").reset_index(drop=True)

    # ----------------
    # DIM_CATEGORY (Violation Type × Dump Description)
    # ----------------
    
    dim_category = (
        pd.DataFrame({
            "ViolationLocatedAt": violation_located,
            "DumpingDescription": dumping_desc
        })
        .drop_duplicates()
        .reset_index(drop=True)
    )
    
    # Generate matching composite key
    dim_category["CategoryKey"] = (
        std_upper_key(dim_category["ViolationLocatedAt"]) + "|" +
        std_upper_key(dim_category["DumpingDescription"])
    )

    # ----------------
    # DIM_INTAKE & DIM_STATUS (Simple Lookup Tables)
    # ----------------
    
    dim_intake = (
        pd.DataFrame({"MethodReceived": fact["MethodReceived"].dropna().drop_duplicates()})
          .sort_values("MethodReceived")
          .reset_index(drop=True)
    )
    
    dim_status = (
        pd.DataFrame({"Status": fact["Status"].dropna().drop_duplicates()})
          .sort_values("Status")
          .reset_index(drop=True)
    )

    return fact, dim_date, dim_location, dim_category, dim_intake, dim_status


# ============================================================================
# QUALITY ASSURANCE & EXPORT
# ============================================================================

def qa_and_export(
    fact, 
    dim_date, 
    dim_location, 
    dim_category, 
    dim_intake, 
    dim_status, 
    export_dir: Path
):
    """
    Validate data model integrity and export to CSV files.
    
    QA Checks:
        1. Fact grain: No duplicate ServiceRequestNumber (ensures 1 row per request)
        2. Join keys: No nulls in LocationKey or CategoryKey (prevents orphan records)
        3. Dimension uniqueness: All dimension keys are unique
    
    If any check fails, raises ValueError before export.
    
    Args:
        fact, dim_date, dim_location, dim_category, dim_intake, dim_status: Model tables
        export_dir: Directory to write CSV files
        
    Raises:
        ValueError: If any QA check fails
    """
    export_dir.mkdir(parents=True, exist_ok=True)

    # ----------------
    # QA Gate 1: Fact Grain
    # ----------------
    dup_sr = fact["ServiceRequestNumber"].duplicated().sum()
    if dup_sr > 0:
        raise ValueError(
            f"Fact grain broken: {dup_sr} duplicate ServiceRequestNumber values found."
        )

    # ----------------
    # QA Gate 2: Required Join Keys
    # ----------------
    for key_col in ["LocationKey", "CategoryKey"]:
        nulls = fact[key_col].isna().sum()
        if nulls > 0:
            raise ValueError(
                f"Null join keys found in fact: {key_col} has {nulls} nulls."
            )

    # ----------------
    # QA Gate 3: Dimension Key Uniqueness
    # ----------------
    if dim_location["LocationKey"].duplicated().any():
        raise ValueError("dim_location has duplicate LocationKey values.")
    if dim_category["CategoryKey"].duplicated().any():
        raise ValueError("dim_category has duplicate CategoryKey values.")

    # ----------------
    # Export CSVs
    # ----------------
    fact.to_csv(export_dir / "fact_illegal_dumping.csv", index=False)
    dim_date.to_csv(export_dir / "dim_date.csv", index=False)
    dim_location.to_csv(export_dir / "dim_location.csv", index=False)
    dim_category.to_csv(export_dir / "dim_category.csv", index=False)
    dim_intake.to_csv(export_dir / "dim_intake.csv", index=False)
    dim_status.to_csv(export_dir / "dim_status.csv", index=False)

    # ----------------
    # Success Report
    # ----------------
    print("✓ BUILD OK")
    print(f"  Exports: {export_dir.resolve()}")
    print(f"  Fact rows: {len(fact):,}")
    print(f"  Dimensions:")
    print(f"    - dim_date: {len(dim_date):,} rows")
    print(f"    - dim_location: {len(dim_location):,} rows")
    print(f"    - dim_category: {len(dim_category):,} rows")
    print(f"    - dim_intake: {len(dim_intake):,} rows")
    print(f"    - dim_status: {len(dim_status):,} rows")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """
    Command-line entry point for star schema builder.
    
    Example usage:
        python build_star_schema.py \\
            --input seattle_illegal_dumping_raw.csv \\
            --export-dir exports/star_schema/
    """
    parser = argparse.ArgumentParser(
        description="Build star schema from Seattle illegal dumping data"
    )
    parser.add_argument(
        "--input", 
        required=True, 
        help="Path to raw Illegal Dumping CSV"
    )
    parser.add_argument(
        "--export-dir", 
        default="exports", 
        help="Directory to write star schema CSVs (default: exports/)"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    export_dir = Path(args.export_dir)

    # Step 1: Load and clean raw data
    print("=" * 60)
    print("STEP 1: Loading and cleaning raw data...")
    print("=" * 60)
    clean_df = load_and_clean(input_path)
    
    quality_report = data_quality_report(clean_df)
    print("\nData Quality Report:")
    print(f"  Total rows: {quality_report['total_rows']:,}")
    print(f"  Total columns: {quality_report['total_columns']}")
    print(f"  Rows with nulls: {quality_report['rows_with_nulls_pct']}%")
    print(f"  Duplicate rows: {quality_report['duplicate_rows']:,}")
    print(f"  Top missing columns: {quality_report['missing_vals_pct_top5']}")

    # Step 2: Build star schema
    print("\n" + "=" * 60)
    print("STEP 2: Building star schema...")
    print("=" * 60)
    fact, dim_date, dim_location, dim_category, dim_intake, dim_status = (
        build_star_schema(clean_df)
    )

    # Step 3: QA and export
    print("\n" + "=" * 60)
    print("STEP 3: Running QA checks and exporting...")
    print("=" * 60)
    qa_and_export(
        fact, dim_date, dim_location, dim_category, dim_intake, dim_status, export_dir
    )
    
    print("\n" + "=" * 60)
    print("COMPLETE: Star schema ready for Power BI import")
    print("=" * 60)


if __name__ == "__main__":
    main()

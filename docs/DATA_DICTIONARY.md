# Data Dictionary

Complete schema reference for the Seattle Illegal Dumping star schema.

Model: Kimball Star Schema
Grain: One row per illegal dumping service request
Records: 266,360 | Period: January 2021 - December 2025
Relationships: All Many-to-One, single-direction (dimension → fact)

---

## Fact Table

### Fact_IllegalDumping (266,360 rows)

| Column | Type | Description |
|--------|------|-------------|
| ServiceRequestID | VARCHAR | Unique request ID -s Primary Key, 100% unique |
| CreatedDateOnly | DATE | Normalised to midnight - FK to Dim_Date[Date] |
| GeographyKey | INTEGER | FK to Dim_Geography |
| DumpTypeKey | INTEGER | FK to Dim_DumpType |
| DumpLocationKey | INTEGER | FK to Dim_DumpLocation |
| StatusKey | INTEGER | FK to Dim_Status |
| IntakeKey | INTEGER | FK to Dim_RequestSource |
| RequestCount | INTEGER | Always 1 - used for SUM aggregations |

Note: CreatedDateOnly is date-only (no time component) to ensure a clean Many-to-One join with Dim_Date. Hour-level analysis uses a separate Hour column retained in the fact table.

---

## Dimension Tables

### Dim_Date (2,191 rows)

Generated via Power Query M. Covers Jan 1 2021 - Dec 31 2026 continuously.
No gaps - required for DATESYTD and SAMEPERIODLASTYEAR to work correctly.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| Date | Date | Primary Key | 2025-01-15 |
| Year | Integer | Calendar year | 2025 |
| Quarter | Integer | 1-4 | 1 |
| Month | Integer | 1-12 | 1 |
| MonthName | Text | Full month name | January |
| MonthShort | Text | Abbreviated | Jan |
| Day | Integer | Day of month | 15 |
| DayOfWeek | Integer | Mon=1 baseline | 3 |
| DayOfWeekName | Text | Full day name | Wednesday |
| IsWeekend | Boolean | True for Sat/Sun | False |
| WeekOfYear | Integer | ISO week 1-53 | 3 |
| DayOfYear | Integer | Day 1-365 | 15 |
| YearMonth | Text | Sort-friendly | 2025-01 |
| YearMonthNum | Integer | Numeric sort key | 202501 |
| QuarterName | Text | Label | Q1 2025 |
| DateKey | Integer | YYYYMMDD surrogate | 20250115 |

---

### Dim_Geography (~30 rows)

Hierarchy: Region → Council District → ZIP Code

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| GeographyKey | Integer | Surrogate PK (Power Query index) | 1 |
| ZIPCode | Text | 5-digit; 6.54% imputed via Census ZCTA | 98107 |
| CouncilDistrict | Integer | Districts 1-7; 0.72% geocoded | 6 |
| Region | Text | NORTH / SOUTH / EAST / WEST / SOUTHWEST | NORTH |
| City | Text | Constant | Seattle |
| State | Text | Constant | WA |

Data Quality: 
ZIP originally 93.45% complete.
After GeoPandas spatial join with Census ZCTA shapefiles: 99.9%. 
(0,0) coordinates treated as missing - masked invalid values.

---

### Dim_DumpType (16 rows)

Composite key ensures each unique type+description gets its own row.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| CategoryKey | Integer | Composite: ViolationType|DumpDescription | 7 |
| DumpType | Text | 16 distinct values | Furniture |
| ViolationType | Text | Broader classification | Bulk Items |
| TypeCategory | Text | General / Bulk / Hazardous / Other | Bulk |

Common values*: Garbage, Furniture, Needles, Litter, Appliances, Tires, Construction Debris, Hazardous Materials

**Key insight: Bulk category (Furniture, Appliances, Tires) is growing despite overall -18% decline - specific service gap identified.

---

### Dim_DumpLocation (14 rows)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| CategoryKey | Integer | Surrogate PK | 3 |
| LocationType | Text | 14 distinct values | Sidewalk |
| LocationCategory | Text | ROW / Public / Private | ROW |

Key insight: Sidewalk is #1 dump location across ALL regions - consistent enforcement focus area.

---

### Dim_Status (7 rows)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| StatusKey | Integer | Surrogate PK | 2 |
| Status | Text | Full status value | Closed |
| StatusCategory | Text | Open / Closed / Duplicate | Closed |
| IsClosed | Boolean | True = request resolved | True |

Status values: Open, Closed, Duplicate (Open), Duplicate (Closed), and 3 additional variants.

Closure Rate: 87.46% - includes Duplicate-Closed as resolved.
233K of 266K requests resolved - benchmark-worthy operations performance.

---

### Dim_RequestSource (6 rows)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| IntakeKey | Integer | Surrogate PK | 1 |
| Source | Text | Intake channel | Call Center |
| SourceCategory | Text | Digital / Phone / Other | Phone |

---

## Relationships

| From (Dimension) | To (Fact) | Cardinality | Direction |
|-----------------|-----------|-------------|-----------|
| Dim_Date[Date] | Fact[CreatedDateOnly] | 1:Many | Single |
| Dim_Geography[GeographyKey] | Fact[GeographyKey] | 1:Many | Single |
| Dim_DumpType[CategoryKey] | Fact[DumpTypeKey] | 1:Many | Single |
| Dim_DumpLocation[CategoryKey] | Fact[DumpLocationKey] | 1:Many | Single |
| Dim_Status[StatusKey] | Fact[StatusKey] | 1:Many | Single |
| Dim_RequestSource[IntakeKey] | Fact[IntakeKey] | 1:Many | Single |

---

## Business Logic

3-Year Growth Calculation:
```
((Requests_2025 - Requests_2022) / Requests_2022) × 100
Baseline filter: ZIPs with ≥50 requests in 2022 only
Result: Overall -18% decline (2022→2025)
```

Closure Rate:
```
Closure Rate = Closed (inc. Duplicate-Closed) / Total Requests
Current: 87.46% (233K of 266K resolved)
```

LocationKey fallback logic:
```
IF lat/long valid (not 0 or null):
    Key = ROUND(lat,4) + "-" + ROUND(long,4)
ELSE:
    Key = UPPER(normalised_address_string)
```

CategoryKey (composite):
```
Key = ViolationType + "|" + DumpDescription
Ensures unique row per type+description combination
```

CreatedDateOnly:
```
Normalised to midnight (00:00:00)
Ensures clean Many-to-One join with Dim_Date[Date]
Hour retained separately for temporal analysis
```

---

## Key Data Findings

| Metric | Value |
|--------|-------|
| Overall 3-year change | -18% (decline) |
| Active backlog | 512 open requests |
| Closure rate | 87.46% |
| Largest category | Garbage (volume) |
| #2 category | Furniture (~62,640 total) |
| Growing category | Bulk Items |
| Top dump location | Sidewalk (all regions) |
| Friday midnight spike | 442 requests - 7× normal |
| Peak reporting hour | 10 AM-12 PM |
| Highest volume region | SOUTH (105,634 visible) |
| Most improved ZIPs | 98107, 98103, 98125 (~99% decline) |
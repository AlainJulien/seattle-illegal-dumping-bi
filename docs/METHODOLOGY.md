# Methodology

Technical approach and design decisions for the Seattle Illegal Dumping BI project.

---

## 1. Data Source

| Attribute | Value |
|-----------|-------|
| Source | Seattle Open Data Portal |
| Dataset | Illegal Dumping Service Requests |
| URL | https://data.seattle.gov/Community/Illegal-Dumping/5ryf-gfu9 |
| Format | CSV export |
| Records | 266,360 |
| Date Range | January 2021 - December 2025 |
| File Size | ~65 MB (cleaned) |
| Duplicates | 0 (100% unique ServiceRequestIDs) |

---

## 2. Star Schema Design

**Methodology**: Kimball Dimensional Modelling

Chosen over normalized (3NF) because:
- Optimized for analytical queries (fewer JOINs)
- Native fit for Power BI single-direction relationships
- Enables stable DAX time intelligence (DATESYTD, SAMEPERIODLASTYEAR)
- Intuitive for business users exploring data

Fact Table Grain: One row per service request

All relationships: Many-to-One (dimension filters fact), single-direction cross-filter - standard for stable DAX time intelligence.

### Dimension Design Decisions

Dim_Date: Generated via Power Query M (not imported from source).
Covers Jan 1 2021 - Dec 31 2026 continuously - no gaps, enabling all time intelligence functions.

Dim_Geography: Hierarchy Region → Council District → ZIP Code.
Enables drill-down on geographic pages. City and State retained as constants ('Seattle', 'WA') for potential multi-city expansion.

Dim_DumpType: Composite key (ViolationType|DumpDescription) ensures each unique type+description combination gets its own dimension row.
TypeCategory standardised to: General / Bulk / Hazardous / Other.

Dim_DumpLocation: 14 distinct location types with LocationCategory grouping (ROW / Public / Private) for aggregation.

Dim_Status: IsClosed boolean flag enables simple closure rate calculation without complex string matching in DAX.

Dim_RequestSource: SourceCategory (Digital / Phone / Other) enables channel shift analysis over time.

Community Reporting Area: Dropped entirely - 76.49% null rate.
Too sparse to include as a dimension; Region → Council District → ZIP provides sufficient geographic granularity.

---

## 3. Data Quality

### Pre-Cleaning Assessment

| Column | % Null (Raw) | Action | Result |
|--------|-------------|--------|--------|
| Community Reporting Area | 76.49% | Dropped from model | Excluded |
| ZIP Code | 6.54% | Census ZCTA spatial join | 99.9% |
| Violation Location | 2.07% | Filled with "Unknown" | 100% |
| Description of Dumping | 1.52% | Filled with "Other" | 100% |
| Council District | 0.72% | Inferred from geocoding | 99.28% |
| Police Precinct | 0.65% | Inferred from geocoding | 99.35% |
| Latitude / Longitude | 0.44% | Retained as-is | ~1,170 records |

### Post-Cleaning Quality Scorecard

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Completeness | 99.1% | All imputation applied |
| Accuracy | 98.5% | No major outliers detected |
| Consistency | 99.8% | All columns standardized |
| Uniqueness | 100% | Zero duplicate ServiceRequestIDs |
| Timeliness | Current | Dates through December 2025 |

---

## 4. Geospatial ZIP Imputation

Problem: 6.54% of records (~17,400) had no ZIP code - critical for geographic analysis.

Solution: GeoPandas spatial join with U.S. Census ZCTA shapefiles.

Process:
1. Load Census ZCTA boundary shapefile (GeoPandas)
2. For records with missing ZIP but valid lat/long coordinates, perform point-in-polygon spatial join
3. Assign matched ZCTA as ZIP code
4. (0,0) coordinates treated as missing - masked invalid values

Result: ZIP completeness 93.45% → 99.9%

Why not drop missing ZIPs?
Dropping 6.54% would introduce geographic bias, potentially removing entire neighborhoods from analysis and skewing regional distributions.

---

## 5. The 3-Year Comparison Decision (2022 vs 2025)

Original plan: Standard YoY comparison (2024 vs 2025)

Problem discovered: 2024 contained only 8 records - a data anomaly, not a real pattern. Any YoY measure using 2024 as baseline would be completely unreliable.

Solution: Pivoted to 3-year window: 2022 (baseline) vs 2025 (current)

Year selection rationale:
- 2021: COVID-19 anomalies distorted patterns
- 2022: First stable post-COVID year - solid baseline (342+ records)
- 2023: Middle year excluded to sharpen contrast
- 2024: Only 8 records - data anomaly
- 2025: Most recent complete year

Key learning: Always validate temporal coverage before committing to time intelligence design. A data quality check in the profiling phase would have caught this earlier.

---

## 6. Baseline Filtering (≥50 Records in 2022)

Rule: ZIPs with fewer than 50 records in 2022 are hidden from geographic ranking visuals.

Why: Prevents percentage inflation from tiny denominators.

Example without filter:
- ZIP A: 4 requests (2022) → 9 requests (2025) = +125% (misleading)
- ZIP B: 300 requests (2022) → 250 requests (2025) = -17% (meaningful)

Without the filter, ZIP A would rank above ZIP B despite being statistically insignificant.

Implementation: Applied at visual level in Power BI using the 'Has Sufficient Baseline 2022' DAX measure - not filtered in the data model, preserving all records for other analyses.

---

## 7. Key Business Logic Rules

Closure Rate: Closed / Total (all statuses).
The 87.46% rate includes Duplicate-Closed as resolved - a deliberate decision since duplicates represent real requests that were handled.

LocationKey: Uses GPS coordinates (4 decimal precision) when available; falls back to normalised address string when lat/long = 0 or null.

CategoryKey: Composite key (ViolationType|DumpDescription) - ensures each unique type+description combination gets its own dimension row.

CreatedDateOnly: Normalised to midnight (date only, no time component) to ensure clean Many-to-One relationships with Dim_Date.

Friday midnight pattern: Hour 0 = timestamps filed between 00:00–00:59. This is NOT when dumping occurs - it is when reports are filed, often the following morning about overnight events. The Friday spike (442 requests, 7× comparable slots) is a commercial dumping signature.

---

## 8. Dashboard Design Decisions

Progressive Disclosure:
- Page 1 (Executive): High-level KPIs for leadership
- Page 2 (Geographic): ZIP-level drill-down for managers
- Page 3 (Category): Dump type breakdown for operations
- Page 4 (Temporal): Day/hour patterns for enforcement scheduling

Map Visual Replaced: The intended geographic heatmap failed due to (0,0) coordinate artefacts. Replaced with a Region Horizontal Bar Chart - simpler, faster-loading, more accessible, and communicates the same regional comparison without coordinate dependency.

Colour Convention:
- Red = High concern / growth against trend
- Green = Improvement / decline (positive outcome here)
- Blue = Neutral / informational

---

## 9. Narrative Reversal

All initial documentation assumed reports were growing (typical urban dumping narrative). After dashboard analysis, the 3-year trend was - 18% - a decline. The entire project narrative was rewritten:

- From: "Crisis intervention needed"
- To: "Success story with targeted remaining challenges"

The counterintuitive finding became the strongest interview talking point, demonstrating critical thinking and genuine insight over confirmation bias.
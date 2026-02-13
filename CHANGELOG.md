\# Changelog



All notable changes to this project will be documented here.



\## \[1.0.0] - February 2026



\### Added

\- ETL pipeline: 266,360 records transformed into star schema

\- Star schema: 1 fact table + 6 dimension tables

\- Geospatial ZIP imputation: 93% → 99.9% completeness

\- Power BI dashboard: 4 pages with 15+ DAX measures

\- Geographic analysis: Top 10 ZIP hotspots identified

\- Temporal analysis: Day/hour heatmap

\- Category analysis: Bulk items, hazmat breakdown

\- CI/CD pipeline: GitHub Actions data validation

\- Full documentation: Data dictionary, DAX library, methodology



\### Fixed

\- Circular reference errors in Power BI dimensions

\- YTD measures not accumulating (date field fix)

\- Small-number bias in growth rankings (≥50 baseline filter)

\- Missing ZIP codes via Census ZCTA spatial join



\## \[Unreleased] - Planned



\### Phase 1 (Q1 2026)

\- Predictive ML models for incident forecasting

\- Anomaly detection for unusual patterns



\### Phase 2 (Q2 2026)

\- Real-time API integration with Seattle Open Data

\- Automated daily refresh pipeline



\### Phase 3 (Q3 2026)

\- Multi-city expansion framework

\- Comparative analysis across West Coast cities


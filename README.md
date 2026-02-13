\# Seattle Illegal Dumping Business Intelligence Analysis



\[!\[Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)

\[!\[Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)](https://powerbi.microsoft.com/)

\[!\[License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

\[!\[Data Quality](https://img.shields.io/badge/Data%20Quality-99.1%25-brightgreen)](docs/METHODOLOGY.md)



> \*\*End-to-end BI solution analysing 266,360 illegal dumping reports in Seattle (2021–2025).

> The headline finding is counterintuitive: requests DECLINED 18% from 2022 to 2025 —

> evidence of successful intervention — while Bulk Items bucked the trend and grew,

> pointing to a specific service gap.\*\*



---



\## Project Overview



This project transforms raw Seattle Public Utilities illegal dumping data into actionable insights through dimensional modelling, advanced DAX analytics, and a 4-page interactive Power BI dashboard.



\### Key Findings



| Finding | Detail |

|---------|--------|

| Overall trend | −18% decline (2022→2025) — a success story |

| Open backlog | Only 512 active requests |

| Closure rate | 87.46% — benchmark-worthy operations |

| Bulk items | Growing despite overall decline ⚠️ |

| Friday midnight | 442 requests — 7× normal (commercial dumping signature) |

| Top ZIPs | 98107, 98103, 98125 — each down ~99% since 2022 |



\### The Counterintuitive Story



Stakeholders assumed illegal dumping was growing. The data showed the opposite: a sustained 18% decline driven by targeted enforcement in high-volume ZIP codes. However, Bulk Items (Furniture, Appliances, Tires) are growing against this trend, pointing to a specific disposal service gap that needs attention.


---



\## Technical Architecture

```

Seattle Open Data Portal (CSV, 266,360 records) 
		↓
Python ETL Pipeline (Pandas + GeoPandas) 
· ZIP imputation via Census ZCTA spatial join

· Composite keys, text standardisation

· Star schema export to CSV
		↓
Power BI Data Model (Kimball Star Schema)
· 1 Fact Table + 6 Dimension Tables

· Power Query M date dimension (2021–2026)

· Single-direction relationships
		↓
DAX Measures (25+ measures)
· 3-year comparison, YTD, rankings, QA
		↓
4-Page Interactive Dashboard
· Executive Summary · Geographic · Category · Temporal

```



---



\## Data Model



\*\*Star Schema — Kimball Methodology\*\*



| Table | Rows | Role |

|-------|------|------|

| Fact\_IllegalDumping | 266,360 | One row per service request |

| Dim\_Date | 2,191 | Full calendar 2021–2026 |

| Dim\_Geography | ~30 | ZIP → Council District → Region |

| Dim\_DumpType | 16 | Dump material + category |

| Dim\_DumpLocation | 14 | Location type + category |

| Dim\_Status | 7 | Request lifecycle status |

| Dim\_RequestSource | 6 | Intake channel |



---



\## Dashboard Pages



\### Page 1 — Executive Summary

\- Total requests, closure rate (87.46%), open backlog (512)

\- 3-year trend: −18% overall decline

\- Summer peaks visible in both 2022 and 2025



\### Page 2 — Geographic Analysis

\- Top ZIPs by 3-year change (baseline filter: ≥50 requests in 2022)

\- NORTH region highest volume but also most dramatic improvements

\- 98107, 98103, 98125 each declined ~99% — intervention success



\### Page 3 — Category Analysis

\- Garbage largest volume; Furniture #2 (~62,640 total)

\- Bulk Items (Furniture, Appliances, Tires) growing against overall trend ⚠️

\- Sidewalk = #1 dump location across ALL regions

\- Needles = 3rd largest category (public health concern)



\### Page 4 — Temporal Analysis

\- Friday midnight: 442 requests at Hour 0 - 7× comparable slots

\- Peak reporting: 10 AM–12 PM (commuters seeing dumping, not when it occurs)

\- Weekly baseline: 500–600/week; summer uptick to 700–800

\- Friday 00:00–04:59 = commercial dumping enforcement window



---



\## Key Technical Challenges Solved



| Challenge | Solution |

|-----------|----------|

| 2024 had only 8 records | Pivoted to 3-year comparison (2022 vs 2025) |

| 6.54% missing ZIP codes | GeoPandas spatial join with Census ZCTA shapefiles |

| YTD non-monotonic behaviour | Added Year context; used KEEPFILTERS in DAX |

| Map visual artefacts | Replaced with regional bar chart |

| Community Reporting Area 76% null | Column dropped from model entirely |

| Narrative assumed growth | Complete rewrite after data revealed decline |



---



\## Skills Demonstrated



\- \*\*ETL\*\*: Python (Pandas, GeoPandas), ZIP imputation, composite key design

\- \*\*Dimensional Modelling\*\*: Kimball star schema, 6 dimensions, grain definition

\- \*\*DAX\*\*: Time intelligence, ranking, baseline filtering, QA measures

\- \*\*Power BI\*\*: 4-page dashboard, Power Query M date dimension, relationships

\- \*\*Business Analysis\*\*: Counterintuitive insight, recommendations, stakeholder narrative

\- \*\*DevOps\*\*: Git, GitHub Actions CI/CD



---



\## Quick Start

```bash

git clone https://github.com/AlainJulien/seattle-illegal-dumping-bi.git

cd seattle-illegal-dumping-bi

python -m venv venv

venv\\Scripts\\activate

pip install -r requirements.txt

python scripts/build\_star\_schema.py

\# Open dashboards/seattle\_dumping\_dashboard.pbix in Power BI Desktop

```



See \[QUICKSTART.md](QUICKSTART.md) for full setup instructions.



---



\## Data Source



\[Seattle Open Data Portal — Illegal Dumping Service Requests](https://data.seattle.gov/Community/Illegal-Dumping/5ryf-gfu9)



\## License



MIT — see \[LICENSE](LICENSE)



\## Contact



\*\*Alain\*\* — Future Business Intelligence Engineer/Analyst

\- GitHub: (https://github.com/AlainJulien)

\- LinkedIn: www.linkedin.com/in/alain-julien


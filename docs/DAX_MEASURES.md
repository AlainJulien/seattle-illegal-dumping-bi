\# DAX Measures Library



\## Base Measures



\*\*Total Requests\*\*

```

Total Requests = SUM(fact\_illegal\_dumping\[request\_count])

```



\*\*Distinct ZIPs\*\*

```

Distinct ZIPs = DISTINCTCOUNT(dim\_location\[zip\_code])

```



---



\## Time Intelligence



\*\*Requests 2022 Total\*\* (baseline year)

```

Requests 2022 Total =

CALCULATE(\[Total Requests], dim\_date\[year] = 2022)

```



\*\*Requests 2025 Total\*\* (comparison year)

```

Requests 2025 Total =

CALCULATE(\[Total Requests], dim\_date\[year] = 2025)

```



\*\*3Yr Change %\*\*

```

3Yr Change % =

VAR Requests2025 = \[Requests 2025 Total]

VAR Requests2022 = \[Requests 2022 Total]

VAR Change = Requests2025 - Requests2022

RETURN

&nbsp;   IF(

&nbsp;       NOT ISBLANK(Requests2022) \&\& Requests2022 > 0,

&nbsp;       DIVIDE(Change, Requests2022, 0),

&nbsp;       BLANK()

&nbsp;   )

```



\*\*Requests YTD\*\*

```

Requests YTD =

VAR MaxDate = MAX(dim\_date\[full\_date])

VAR MaxDayOfYear = CALCULATE(MAX(dim\_date\[day\_of\_year]),

&nbsp;                  dim\_date\[full\_date] = MaxDate)

RETURN

&nbsp;   CALCULATE(

&nbsp;       \[Total Requests],

&nbsp;       FILTER(ALL(dim\_date),

&nbsp;           dim\_date\[year] = YEAR(MaxDate) \&\&

&nbsp;           dim\_date\[day\_of\_year] <= MaxDayOfYear

&nbsp;       )

&nbsp;   )

```



---



\## Ranking



\*\*Has Sufficient Baseline\*\*

```

Has Sufficient Baseline =

IF(\[Requests 2022 Total] >= 50, "Yes", "No")

```



\*\*ZIP 3Yr Rank\*\*

```

ZIP 3Yr Rank =

IF(

&nbsp;   \[Has Sufficient Baseline] = "Yes",

&nbsp;   RANKX(

&nbsp;       FILTER(ALL(dim\_location\[zip\_code]),

&nbsp;              \[Has Sufficient Baseline] = "Yes"),

&nbsp;       \[3Yr Change %],,DESC,DENSE

&nbsp;   ),

&nbsp;   BLANK()

)

```



---



\## Category Measures



\*\*Bulk Items Requests\*\*

```

Bulk Items Requests =

CALCULATE(\[Total Requests],

&nbsp;         dim\_category\[category\_name] = "Bulk Items")

```



\*\*Hazmat Requests\*\*

```

Hazmat Requests =

CALCULATE(\[Total Requests],

&nbsp;         dim\_category\[is\_hazardous] = TRUE)

```



---



\## Best Practices

\- Always use VAR for complex calculations

\- Use DIVIDE() instead of / to avoid division by zero

\- Return BLANK() for missing data (not 0)

\- Use DENSE ranking to avoid gaps


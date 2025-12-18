# Citi Bike Analysis Notebooks

Analysis notebooks for the NYC Citi Bike dataset (287M trips, 2013-2025).

## Main Notebooks

| Notebook | Description | Data Required |
|----------|-------------|---------------|
| `explore_data.ipynb` | Basic data exploration and sanity checks | NYC parquet |
| `demographics_analysis.ipynb` | Age/gender analysis using legacy data (2013-2019) | NYC parquet (birth_year_valid) |
| `identifying_interns_students.ipynb` | Identifies summer interns vs students using age + location + timing | NYC parquet (2017-2019) |
| `wfh_and_patterns.ipynb` | Work-from-home impact analysis (pre/post COVID) | NYC parquet |
| `routes_and_corridors.ipynb` | Popular routes and corridor analysis | NYC parquet |
| `cross_hudson_analysis.ipynb` | NYC to Jersey City cross-Hudson trip patterns | NYC + JC parquet |
| `jc_exploration.ipynb` | Jersey City/Hoboken data exploration | JC parquet |
| `comprehensive_analysis.ipynb` | Full multi-year trend analysis | NYC parquet |
| `demand_adjusted_growth.ipynb` | Growth analysis controlling for station expansion | NYC parquet |
| `weather_adjusted_growth.ipynb` | Growth analysis controlling for weather | NYC + weather parquet |

## Key Findings

- **Intern/Student Detection**: 144pp difference-in-differences between banks and universities for 20-21 year olds proves seasonal employment patterns
- **Cross-Hudson**: 7.35M trips from NYC to JC over 12 years (2.56% of all NYC trips)
- **WFH Impact**: Significant shift in commute patterns post-2020

## Archive

The `archive/` folder contains exploratory notebooks that were consolidated into the main analysis notebooks above.

## Running Notebooks

```bash
cd notebooks
jupyter notebook <notebook_name>.ipynb
```

All notebooks expect to be run from the `notebooks/` directory and use relative paths like `../data/processed/*.parquet`.

# INT315 Sales Analysis

An academic retail sales analysis project using Apache Spark, Scala, Spark SQL, RDDs, and GraphX. The project cleans the Online Retail dataset, calculates sales metrics, produces analytical CSV outputs, and displays the results in a browser dashboard.

## Project workflow

1. `src/SalesAnalysis.scala` loads `data/OnlineRetail.csv`.
2. Invalid records are removed, including cancelled invoices, missing descriptions, non-positive quantities, and non-positive prices.
3. Revenue is calculated as `Quantity × UnitPrice`.
4. Spark RDD operations, Spark SQL queries, and a country-product GraphX network are run.
5. Results are written to subdirectories under `output/`.
6. `prepare_dashboard.py` reads those Spark CSV outputs and generates `dashboard_data.js`.
7. `dashboard.html` loads the generated data and presents KPIs, charts, tables, and GraphX relationship statistics.

## Prerequisites

- Java runtime compatible with the installed Apache Spark version
- Apache Spark 3.5.x
- Scala compatible with the Spark installation
- Python 3.8 or newer
- A browser and an internet connection for the Chart.js charts

No Python packages are required. The dashboard uses Chart.js from jsDelivr.

## Running the analysis

The Scala source currently contains absolute Windows paths pointing to the original project directory:

```text
C:/Users/alfre/INT315-Sales-Analysis/
```

Update those paths in `src/SalesAnalysis.scala` if the repository is stored elsewhere. Then compile and run `SalesAnalysis` using the Spark and Scala toolchain installed on your machine. For example, after packaging the class into a JAR:

```powershell
spark-submit --class SalesAnalysis --master "local[*]" path\to\sales-analysis.jar
```

The analysis writes the following outputs:

- `output/cleaned_sales/` - cleaned line-item records
- `output/sql_summary/` - overall sales KPIs
- `output/sql_top_products/` - top products by revenue
- `output/sql_country_sales/` - top countries by revenue
- `output/sql_monthly_sales/` - monthly sales trends
- `output/graphx_results/` - highest-value country-product relationships

Spark writes CSV files into these directories as `part-*.csv` files. Do not read the `_SUCCESS` marker as data.

## Preparing and viewing the dashboard

After the Spark analysis completes, run the dashboard preparation script from the repository root:

```powershell
python prepare_dashboard.py
```

This regenerates `dashboard_data.js` from the Spark output directories. Serve the repository through a local web server so the dashboard behaves consistently:

```powershell
python -m http.server 8000
```

Open [http://localhost:8000/dashboard.html](http://localhost:8000/dashboard.html) in a browser.

The dashboard reports values in GBP and includes:

- total sales value, valid sales lines, units sold, and average line value
- monthly sales trends
- top products and countries
- top product revenue and unit tables
- GraphX vertex, edge, and highest-value relationship summaries

## Repository structure

```text
.
├── data/OnlineRetail.csv       # Source retail transactions
├── src/SalesAnalysis.scala     # Spark cleaning and analysis pipeline
├── prepare_dashboard.py        # Converts Spark CSVs to dashboard_data.js
├── dashboard.html              # Static dashboard application
├── dashboard_data.js           # Generated dashboard data
└── output/                     # Spark-generated CSV results
```

`dashboard_data.js` and the contents of `output/` are generated artifacts. Regenerate them whenever the source data or Scala analysis changes.

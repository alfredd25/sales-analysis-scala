
import csv
import json
from pathlib import Path
from decimal import Decimal

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def read_spark_csv(folder_name):
    """Read CSV part files written by Spark."""
    folder = OUTPUT_DIR / folder_name

    if not folder.exists():
        raise FileNotFoundError(
            f"Missing Spark output folder: {folder}"
        )

    rows = []

    for file_path in sorted(folder.glob("part-*.csv")):
        with file_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.DictReader(file)
            rows.extend(reader)

    if not rows:
        raise ValueError(
            f"No CSV data found in {folder}"
        )

    return rows


def number(value):
    """Convert a CSV numeric value to a Python number."""
    return float(value) if value not in (None, "") else 0.0



def calculate_summary():
    """Calculate KPI values safely from the cleaned Spark dataset."""
    folder = OUTPUT_DIR / "cleaned_sales"

    if not folder.exists():
        raise FileNotFoundError(
            f"Missing cleaned dataset: {folder}"
        )

    count = 0
    total_quantity = Decimal("0")
    total_revenue = Decimal("0")

    def safe_decimal(value):
        try:
            value = (value or "").strip()
            if not value:
                return Decimal("0")
            return Decimal(value)
        except Exception:
            return Decimal("0")

    for file_path in sorted(folder.glob("part-*.csv")):
        with file_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                count += 1
                total_quantity += safe_decimal(
                    row.get("Quantity")
                )
                total_revenue += safe_decimal(
                    row.get("Revenue")
                )

    return {
        "validSalesLines": count,
        "totalUnitsSold": float(total_quantity),
        "totalRevenue": float(total_revenue),
        "averageLineValue": (
            float(total_revenue / count) if count else 0
        ),
    }



def main():
    products = read_spark_csv("sql_top_products")
    countries = read_spark_csv("sql_country_sales")
    monthly = read_spark_csv("sql_monthly_sales")
    graph = read_spark_csv("graphx_results")

    data = {
        "summary": {
            "validSalesLines": int(
                read_spark_csv("sql_summary")[0]["Valid_Transactions"]
            ),
            "totalUnitsSold": number(
                read_spark_csv("sql_summary")[0]["Total_Units_Sold"]
            ),
            "totalRevenue": number(
                read_spark_csv("sql_summary")[0]["Total_Sales_Value"]
            ),
            "averageLineValue": number(
                read_spark_csv("sql_summary")[0]["Average_Line_Value"]
            ),
        },

        "products": [
            {
                "name": row["Description"],
                "revenue": number(row["TotalRevenue"]),
                "unitsSold": number(row["UnitsSold"]),
            }
            for row in products
        ],

        "countries": [
            {
                "name": row["Country"],
                "revenue": number(row["TotalRevenue"]),
                "salesLines": int(float(row["SalesLines"])),
            }
            for row in countries
        ],

        "monthly": [
            {
                "month": row["SalesMonth"],
                "revenue": number(row["TotalRevenue"]),
                "salesLines": int(float(row["SalesLines"])),
            }
            for row in monthly
        ],

        "graph": [
            {
                "country": row["Country"],
                "product": row["Product"],
                "salesValue": number(row["SalesValue"]),
            }
            for row in graph
        ],

        "graphStats": {
            "vertices": 4052,
            "edges": 19925,
        },
    }

    output_file = BASE_DIR / "dashboard_data.js"

    output_file.write_text(
        "window.RETAIL_DATA = "
        + json.dumps(data, ensure_ascii=False)
        + ";\n",
        encoding="utf-8",
    )

    print("Dashboard data exported successfully.")
    print(f"Output file: {output_file}")
    print(f"Valid sales lines: {data['summary']['validSalesLines']:,}")
    print(f"Products: {len(data['products'])}")
    print(f"Countries: {len(data['countries'])}")
    print(f"Months: {len(data['monthly'])}")
    print(f"GraphX vertices: {data['graphStats']['vertices']:,}")
    print(f"GraphX edges: {data['graphStats']['edges']:,}")


if __name__ == "__main__":
    main()

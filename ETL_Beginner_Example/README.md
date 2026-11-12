# Beginner ETL Example

This example uses Python's built-in libraries, so no packages need to be installed.

New to Python syntax? Start with the workspace's [Python Syntax Guide](../Python_Syntax_Guide.md).

## Folder contents

- `data/sales.csv` — sample input data, including a few intentionally invalid rows
- `etl_pipeline.py` — the Extract, Transform, and Load program
- `output/` — created automatically when the program runs

## Run it

Open a terminal in this folder and run:

```powershell
python etl_pipeline.py
```

The script prints the number of rows read, loaded, and rejected. It creates:

- `output/clean_sales.csv` — valid rows, with dates checked and standardized
- `output/rejected_sales.csv` — rejected rows and a reason for each rejection

The example keeps the first valid row for each `order_id`. It rejects rows with a missing ID, a duplicate ID, a date not written as `YYYY-MM-DD`, a missing or invalid amount, or a negative amount.

## Follow the ETL steps in the code

1. **Extract:** `extract()` reads the input CSV and checks its columns.
2. **Transform:** `transform()` validates rows, keeps valid rows, and records why invalid rows were rejected.
3. **Load:** `load()` writes both result files.

Try changing `data/sales.csv` and running the program again. The output files are overwritten each time, so each run starts with a fresh result.

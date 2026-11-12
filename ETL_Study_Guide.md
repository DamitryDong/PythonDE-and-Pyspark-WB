# ETL Study Guide: Start with the Basics

This guide introduces ETL from the beginning. You do not need previous data-engineering experience to start.

## 1. What is data?

Data is information stored in a form a computer can work with. Examples include:

- A row in a spreadsheet describing one customer
- A CSV file containing daily sales
- A database table containing product information
- A JSON response from a web service

A **dataset** is a collection of related data. A sales dataset, for example, might have one row per order and columns such as `order_id`, `order_date`, and `amount`.

## 2. What does ETL mean?

**ETL** stands for:

1. **Extract** — get data from a source.
2. **Transform** — check, clean, reshape, or enrich the data.
3. **Load** — put the prepared data somewhere it can be used.

For example, a company might extract sales rows from CSV files, transform dates and amounts into consistent formats, and load the results into a database for reporting.

```text
CSV files  --extract-->  Python process  --transform-->  Database
                                                    \--load--/
```

The main goal is to move data reliably from where it is produced to where it is needed.

## 3. The three ETL steps

### Extract

Extraction reads data from one or more sources. Common sources include:

- Files: CSV, JSON, Excel, or text
- Databases
- APIs (services that provide data over the web)
- Cloud storage

Questions to ask:

- Where does the data come from?
- What format is it in?
- How much data is there?
- How often does it change?

### Transform

Transformation makes data consistent and useful. Typical tasks include:

- Removing extra spaces
- Converting text dates into date values
- Converting amounts from text into numbers
- Handling missing or invalid values
- Removing duplicate records
- Renaming columns
- Combining related datasets

Transformations should follow clear rules. For instance, decide what to do when an order has no amount instead of silently treating it as zero.

### Load

Loading writes the prepared data to a destination, such as:

- A CSV file
- A database table
- A data warehouse
- A cloud storage location

The destination is sometimes called the **target**. The original source is sometimes called the **source system**.

## 4. A tiny example

Imagine this source file, `sales.csv`:

```csv
order_id,amount
1001,12.50
1002,7.00
```

The goal is to load the rows into a database table after checking that each amount is numeric and non-negative.

In plain language, the pipeline is:

```text
Extract: read sales.csv
Transform: check each amount and prepare clean rows
Load: insert the clean rows into a sales table
```

Here is a deliberately small Python illustration of the read-and-check part:

```python
import csv
from decimal import Decimal

clean_rows = []

with open("sales.csv", newline="", encoding="utf-8") as file:
    for row in csv.DictReader(file):
        amount = Decimal(row["amount"])
        if amount < 0:
            raise ValueError(f"Negative amount for order {row['order_id']}")
        clean_rows.append({
            "order_id": row["order_id"],
            "amount": amount,
        })
```

This example extracts rows and performs a basic validation. It does **not** load them into a database yet. Keeping the steps distinct makes a pipeline easier to understand, test, and troubleshoot.

For a complete runnable version, see the [Beginner ETL Example](./ETL_Beginner_Example/README.md). It reads a sample CSV, separates valid and rejected rows, and writes both results to output files.

## 5. Important beginner concepts

- **Pipeline:** The sequence of steps that moves data from source to destination.
- **Record / row:** One item in a dataset, such as one order.
- **Field / column:** One property of a record, such as `order_id`.
- **Schema:** The expected structure of data, including its columns and types.
- **Data type:** The kind of a value, such as text, integer, decimal, or date.
- **Validation:** Checking whether data meets expectations.
- **Null / missing value:** A value that is absent or unknown.
- **Duplicate:** A record that appears more than once when it should be unique.
- **Batch:** A group of records processed together.
- **Log:** A record of what a program did, useful for understanding failures.

## 6. ETL and ELT

You may also see **ELT**:

- **ETL:** Transform data before loading it into the target.
- **ELT:** Load the raw data first, then transform it inside the target system.

Both approaches move and prepare data. The choice depends on the tools, data size, and design of the system. Learn the ETL steps first; the same ideas help you understand ELT.

## 7. A good first-pipeline checklist

Before building a pipeline, answer:

1. What is the source?
2. What output should be produced, and where?
3. What does each input column mean?
4. Which values are required, and which can be missing?
5. How should invalid rows be handled?
6. How will you know the run succeeded?
7. Can you safely run it again without creating unwanted duplicates?

For a first exercise, it is fine to stop after writing the cleaned output to a new CSV file. That lets you learn extraction and transformation before adding a database.

## 8. Suggested beginner learning path

1. Learn basic Python: variables, strings, numbers, lists, dictionaries, loops, and functions.
2. Read and write CSV files with Python's built-in `csv` module.
3. Inspect data: count rows, check columns, and look for missing or duplicate values.
4. Add simple transformations and validations.
5. Write the cleaned data to a new file.
6. Learn SQL basics: `SELECT`, `WHERE`, `INSERT`, and table structure.
7. Load data into a local database, such as SQLite.
8. Add logging, error handling, and repeatable runs.
9. Later, explore scheduling, larger datasets, and orchestration tools.

## 9. First practice exercise

Create a small `sales.csv` file with these columns:

```text
order_id,order_date,amount
```

Add a few rows, including one row with a blank amount and one duplicate `order_id`. Then write a program that:

1. Reads the file.
2. Counts the input rows.
3. Identifies blank amounts and duplicate order IDs.
4. Keeps valid rows and writes them to `clean_sales.csv`.
5. Prints how many rows were read, kept, and rejected.

Make a decision about invalid rows and document it. For example, you could skip them and report why, rather than silently dropping them.

## 10. Quick recap

ETL is a practical way to move data:

- **Extract** it from a source.
- **Transform** it according to clear rules.
- **Load** it into a useful destination.

Start with small files, inspect the input, make transformations explicit, and check the output. A reliable pipeline is not just one that runs; it is one whose results you can explain and trust.

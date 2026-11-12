"""A small CSV-to-CSV ETL pipeline for learning Python and data processing.

Extract: read the source CSV.
Transform: validate each row and separate clean rows from rejected rows.
Load: write both groups to output CSV files.
"""

# csv provides tools for reading and writing comma-separated files.
import csv

# datetime parses and validates dates.
from datetime import datetime

# Decimal is suitable for decimal amounts such as prices. InvalidOperation
# is raised when a string cannot be represented as a valid Decimal number.
from decimal import Decimal, InvalidOperation

# Path provides a cross-platform way to build and use file paths.
from pathlib import Path


# __file__ is the path to this script. Using its parent directory means the
# program can find its data and output folders regardless of the terminal's
# current working directory.
BASE_DIR = Path(__file__).parent
SOURCE_FILE = BASE_DIR / "data" / "sales.csv"
OUTPUT_DIR = BASE_DIR / "output"
CLEAN_FILE = OUTPUT_DIR / "clean_sales.csv"
REJECTED_FILE = OUTPUT_DIR / "rejected_sales.csv"

# A set is useful here because we only need to check whether a column name
# exists; we do not need to preserve the order of required column names.
REQUIRED_COLUMNS = {"order_id", "order_date", "amount"}

# CSV output columns are lists so their written order is predictable.
OUTPUT_COLUMNS = ["order_id", "order_date", "amount"]
REJECTED_COLUMNS = OUTPUT_COLUMNS + ["rejection_reason"]


def extract(source_file):
    """Read sales rows and ensure the CSV has the required columns.

    Returns a list of dictionaries. Each dictionary represents one CSV row,
    with column names as keys and cell contents as string values.
    """
    # The "with" statement closes the file automatically, even if an error
    # happens while reading it. newline="" is the recommended CSV setting.
    with source_file.open("r", newline="", encoding="utf-8") as file:
        # DictReader uses the first CSV row as dictionary keys (column names).
        reader = csv.DictReader(file)

        # A missing fieldnames value means the file did not contain a header.
        if reader.fieldnames is None:
            raise ValueError(f"{source_file} is empty or has no header row.")

        # Set.difference returns required column names that are not in the file.
        missing_columns = REQUIRED_COLUMNS.difference(reader.fieldnames)
        if missing_columns:
            # Sort the set to make the error message stable and easy to read.
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"The source CSV is missing required columns: {missing}")

        # DictReader is an iterator; list(...) reads all its rows into memory.
        return list(reader)


def transform(rows):
    """Validate rows and return separate clean and rejected lists.

    This example rejects invalid rows rather than guessing how to repair them.
    """
    # Each list accumulates rows for one of the two output files.
    clean_rows = []
    rejected_rows = []

    # A set tracks IDs already accepted as valid, making duplicate checks easy.
    seen_order_ids = set()

    # Process one row at a time. Values read from CSV files start as strings.
    for row in rows:
        # .get() safely looks up a column. "or ''" handles a missing/None cell;
        # strip() removes whitespace from the beginning and end of the value.
        order_id = (row.get("order_id") or "").strip()
        order_date = (row.get("order_date") or "").strip()
        amount_text = (row.get("amount") or "").strip()

        # The if/elif/else chain selects the first validation failure found.
        if not order_id:
            reason = "order_id is missing"
        elif order_id in seen_order_ids:
            reason = f"duplicate order_id: {order_id}"
        else:
            # strptime parses text using the specified format. If the date is
            # cannot be parsed with this date format, it raises ValueError.
            try:
                parsed_date = datetime.strptime(order_date, "%Y-%m-%d").date()
            except ValueError:
                reason = "order_date must use YYYY-MM-DD format"
            else:
                # Decimal converts the input text to an exact decimal value,
                # avoiding common floating-point rounding surprises with money.
                try:
                    amount = Decimal(amount_text)

                    # Decimal accepts special values such as NaN and Infinity;
                    # this pipeline considers those invalid amounts.
                    if not amount.is_finite():
                        raise InvalidOperation
                except InvalidOperation:
                    reason = "amount must be a valid number"
                else:
                    # A zero amount is allowed; only amounts below zero fail.
                    if amount < 0:
                        reason = "amount cannot be negative"
                    else:
                        # The row passed every check. Store normalized values:
                        # ISO date text and a string version of the Decimal.
                        clean_rows.append({
                            "order_id": order_id,
                            "order_date": parsed_date.isoformat(),
                            "amount": str(amount),
                        })

                        # Record the ID only after accepting the row. This means
                        # a rejected row does not block a later valid row that
                        # happens to use the same ID.
                        seen_order_ids.add(order_id)

                        # This row is done, so skip the rejection code below
                        # and move directly to the next input row.
                        continue

        # If validation reached here, "reason" describes why the row failed.
        # Keep the original text values to help a person correct the source.
        rejected_rows.append({
            "order_id": order_id,
            "order_date": order_date,
            "amount": amount_text,
            "rejection_reason": reason,
        })

    return clean_rows, rejected_rows


def load(clean_rows, rejected_rows):
    """Write valid rows and rejected rows to separate output CSV files."""
    # Create output/ if it does not exist. exist_ok=True makes repeat runs safe.
    OUTPUT_DIR.mkdir(exist_ok=True)

    # "w" creates a new file or replaces the previous output file.
    with CLEAN_FILE.open("w", newline="", encoding="utf-8") as file:
        # DictWriter maps each dictionary's values to the named CSV columns.
        writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
        # Include the column-name header as the first row in the output.
        writer.writeheader()
        # Write every clean dictionary as one CSV data row.
        writer.writerows(clean_rows)

    # Rejected rows get their own file, including the validation reason.
    with REJECTED_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REJECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(rejected_rows)


def main():
    """Run the full Extract -> Transform -> Load pipeline."""
    # Extract: read rows from the sample source file.
    rows = extract(SOURCE_FILE)

    # Transform: return two results, unpacked into separate variables.
    clean_rows, rejected_rows = transform(rows)

    # Load: save both results to their respective output files.
    load(clean_rows, rejected_rows)

    # f-strings insert values inside {} into a readable message.
    print(f"Rows read: {len(rows)}")
    print(f"Rows loaded: {len(clean_rows)}")
    print(f"Rows rejected: {len(rejected_rows)}")
    print(f"Clean output: {CLEAN_FILE}")
    print(f"Rejected output: {REJECTED_FILE}")


# Python sets __name__ to "__main__" when this file is run directly. This
# guard lets the functions be imported elsewhere without running the pipeline.
if __name__ == "__main__":
    main()

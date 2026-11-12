# Beginner Python Syntax Guide

This guide covers Python syntax used in the [ETL beginner example](./ETL_Beginner_Example/etl_pipeline.py). You can read it alongside the code.

## 1. Python files and indentation

Python programs are saved in files ending in `.py`. Indentation (spaces at the beginning of a line) groups statements together. Four spaces per indentation level is the usual convention.

```python
if 3 > 2:
    print("This line belongs to the if block.")
```
Javascript 
if (3 > 2) {
    console.log("dadad")
}
The indented line only runs when the condition is true. Use consistent indentation; Python uses it to understand the program structure.

## 2. Comments and docstrings

Use `#` for a comment. Python ignores the rest of that line:

```python
# This is a short comment.
amount = 12.50
```

A docstring is a string at the beginning of a file or function that explains its purpose:

```python
def greet():
    """Display a greeting."""
    print("Hello!")

const greet = () = {
    /*dadadadadadada*/
    console.log("helloe)
}
```

Comments and docstrings help people understand why code exists or how to use it.

## 3. Variables and values

A variable is a name that refers to a value. The `=` sign assigns a value:

```python
customer_name = "Avery"  # str: text
order_count = 3          # int: whole number
amount = 12.50           # float: decimal approximation
is_valid = True          # bool: True or False
```

In the ETL example, names such as `clean_rows` and `source_file` hold data used by the program. Names are case-sensitive: `amount` and `Amount` are different names.

## 4. Common collection types

### Lists

A list stores an ordered group of values. Square brackets create a list:

```python
columns = ["order_id", "order_date", "amount"]
columns.append("customer_name")
```

Lists preserve order and can be changed. The ETL program uses lists to collect rows and define output column order.

### Dictionaries

A dictionary stores values under named keys. Curly braces define its key-value pairs:

```python
order = {"order_id": "1001", "amount": "12.50"}
print(order["order_id"])
```

CSV rows in the example are dictionaries: the column names are keys, and the cell contents are values.

Use `.get()` when a key may not be present:

```python
amount = order.get("amount")
```

Unlike `order["amount"]`, `.get()` returns `None` when the key is missing rather than raising a `KeyError`.

### Sets

A set stores unique values and is useful for membership checks:

```python
seen_ids = set()
seen_ids.add("1001")

if "1001" in seen_ids:
    print("This ID has already been seen.")
```

The ETL example uses a set to detect duplicate order IDs.

## 5. Strings and string methods

Text values are strings. They can use single or double quotes:

```python
first_name = "Avery"
last_name = 'Lee'

```

Methods perform useful operations on strings:

```python
raw_value = "  12.50  "
clean_value = raw_value.strip()  # "12.50"
```

The `.strip()` method removes whitespace from both ends of a string. It does not remove spaces from the middle.

An **f-string** puts expressions inside curly braces:

```python
order_id = "1001"
message = f"Processing order {order_id}"
```

The ETL example uses f-strings to include file paths, IDs, and row counts in messages.

## 6. Operators and comparisons

Operators calculate values or compare them:

```python
total = 10 + 5
is_free = total == 0
is_negative = total < 0
```

Common comparison operators:

| Operator | Meaning |
|---|---|
| `==` | equal to |
| `!=` | not equal to |
| `<` | less than |
| `<=` | less than or equal to |
| `>` | greater than |
| `>=` | greater than or equal to |

Use `==` to compare values. A single `=` assigns a value to a variable.

## 7. Conditions

`if`, `elif`, and `else` choose which block to run:

```python
if amount < 0:
    result = "negative"
elif amount == 0:
    result = "zero"
else:
    result = "positive"
```

Python checks conditions from top to bottom and runs the first matching block. The ETL example uses this pattern to choose a rejection reason.

## 8. Loops

A `for` loop processes each item in a collection:

```python
order_ids = ["1001", "1002"]

for order_id in order_ids:
    print(order_id)
```

The loop variable (`order_id`) refers to the current item. In the ETL program, `for row in rows:` processes one CSV row at a time.

`continue` skips the rest of the current loop iteration and starts the next one. The example uses it after accepting a valid row so that row is not also added to the rejected results.

## 9. Functions

A function is a reusable block of code. Define one with `def`, give it a name, and put its parameters in parentheses:

```python
def add_tax(amount, rate):
    total = amount + amount * rate
    return total

price = add_tax(10, 0.1)
```

- `amount` and `rate` are **parameters**: inputs to the function.
- `add_tax(10, 0.1)` is a **function call**.
- `return` sends a result back to the caller.

In the ETL program, `extract`, `transform`, `load`, and `main` each have a separate job.

## 10. Imports and modules

An import makes code from another module available in your file:

```python
import csv
from pathlib import Path
```

Python includes many standard-library modules, so they do not need separate installation. The ETL example uses `csv`, `datetime`, `decimal`, and `pathlib`.

## 11. Working with files

The `with` statement safely manages a file. When the block ends, Python closes the file:

```python
with open("notes.txt", "r", encoding="utf-8") as file:
    contents = file.read()
```

Common file modes include:

- `"r"` — read
- `"w"` — write, replacing existing contents
- `"a"` — append to the end

The ETL example uses `Path` to build file paths:

```python
from pathlib import Path

folder = Path("data")
file_path = folder / "sales.csv"
```

The `/` between `Path` values joins path parts in a platform-aware way. It is not dividing numbers in this context.

## 12. Errors and exceptions

An exception represents an error while the program runs. `try` and `except` allow a program to handle an expected kind of error:

```python
try:
    number = int("abc")
except ValueError:
    print("That text is not a whole number.")
```

`raise` creates an exception when the program cannot continue safely:

```python
if not file_exists:
    raise ValueError("The input file was not found.")
```

The ETL example handles invalid dates and amounts, and raises a clear error if required CSV columns are missing.

## 13. Converting values

Values read from a CSV file are text. Convert them before doing calculations or date checks:

```python
count = int("3")
price = Decimal("12.50")
```

Use `Decimal` for values like money when exact decimal arithmetic is important. The example converts date text with `datetime.strptime(...)` and converts it back to a standard string with `.isoformat()`.

## 14. The main guard

You may see this at the bottom of a Python file:

```python
if __name__ == "__main__":
    main()
```

When Python runs a file directly, it sets `__name__` to `"__main__"`, so `main()` runs. If another file imports this file, the condition is false and the pipeline does not start automatically.

## 15. Run the ETL example

Open a terminal in the `ETL_Beginner_Example` folder and run:

```powershell
python etl_pipeline.py
```

Then compare the source file with the files created in `output/`. To learn more, read the [ETL study guide](./ETL_Study_Guide.md) and the example's [README](./ETL_Beginner_Example/README.md).

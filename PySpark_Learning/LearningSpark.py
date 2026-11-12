r"""
PySpark basics: a commented, runnable starting point.

Run this file with the Python interpreter where PySpark is installed:
    .\.venv\Scripts\python.exe .\LearningSpark.py

The examples use a tiny in-memory employee dataset, so no input files are
needed. Spark can also read and write files; see the commented examples below.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def main() -> None:
    # SparkSession is the entry point for using Spark SQL and DataFrames.
    # master("local[*]") runs Spark on this computer, using available CPU cores.
    # On a company Spark cluster, the cluster configuration is provided
    # differently; you generally remove the local master setting.
    spark = (
        SparkSession.builder
        .appName("LearningSparkBasics")
        .master("local[*]")
        .getOrCreate()
    )

    # Spark can print useful startup messages. Set the level to WARN to reduce
    # routine informational output while learning.
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ------------------------------------------------------------------
        # 1. Create a DataFrame and understand its schema
        # ------------------------------------------------------------------

        # A schema names each column and defines its data type.
        # Explicit schemas are safer than asking Spark to guess types.
        employee_schema = StructType(
            [
                StructField("employee_id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("department", StringType(), nullable=False),
                StructField("salary", IntegerType(), nullable=True),
                StructField("is_active", BooleanType(), nullable=False),
                StructField("manager", StringType(), nullable=True),
            ]
        )

        # Each dictionary represents one row. None represents a missing value
        # (Spark SQL NULL). This dataset lives in memory; it is not a file.
        employee_data = [
            {
                "employee_id": 1,
                "name": "Ava",
                "department": "Engineering",
                "salary": 95000,
                "is_active": True,
                "manager": "Mina",
            },
            {
                "employee_id": 2,
                "name": "Noah",
                "department": "Engineering",
                "salary": 88000,
                "is_active": True,
                "manager": "Mina",
            },
            {
                "employee_id": 3,
                "name": "Mia",
                "department": "Sales",
                "salary": 72000,
                "is_active": True,
                "manager": None,
            },
            {
                "employee_id": 4,
                "name": "Leo",
                "department": "Sales",
                "salary": None,
                "is_active": False,
                "manager": "Omar",
            },
            {
                "employee_id": 5,
                "name": "Zoe",
                "department": "Human Resources",
                "salary": 68000,
                "is_active": True,
                "manager": "Omar",
            },
        ]
        employees = spark.createDataFrame(employee_data, schema=employee_schema)

        # show() displays rows; printSchema() displays column names and types.
        employees.show()
        employees.printSchema()

        # count() is an action: it asks Spark to execute work and return a value.
        print(f"Employee rows: {employees.count()}")
        print(f"Column names: {employees.columns}")

        # ------------------------------------------------------------------
        # 2. Select, filter, and sort rows and columns
        # ------------------------------------------------------------------

        # select() chooses columns. Use F.col() to refer to a column by name.
        employees.select("name", "department", "salary").show()
        employees.select(F.col("name"), F.col("salary")).show()

        # filter() (also called where()) keeps rows that meet a condition.
        # Use &, |, and ~ for AND, OR, and NOT; put each condition in parentheses.
        engineering = employees.filter(F.col("department") == "Engineering")
        engineering.show()

        active_high_earners = employees.filter(
            (F.col("is_active") == True) & (F.col("salary") >= 80000)
        )
        active_high_earners.show()

        # isin() checks membership in a list; desc() sorts descending.
        selected_departments = employees.filter(
            F.col("department").isin("Engineering", "Sales")
        )
        selected_departments.orderBy(F.col("salary").desc()).show()

        # ------------------------------------------------------------------
        # 3. Add, change, and remove columns
        # ------------------------------------------------------------------

        # withColumn() returns a new DataFrame; it does not change employees.
        # F.when() is similar to an if/else expression performed by Spark.
        with_bonus_category = employees.withColumn(
            "salary_band",
            F.when(F.col("salary") >= 90000, "high").otherwise("standard"),
        )
        with_bonus_category.select("name", "salary", "salary_band").show()

        # Rename one column. drop() removes a column from the result.
        renamed = employees.withColumnRenamed("name", "employee_name")
        without_manager = renamed.drop("manager")
        without_manager.show()

        # cast() changes a column's data type. Here the ID becomes text.
        employees.withColumn(
            "employee_id_text", F.col("employee_id").cast("string")
        ).select("employee_id", "employee_id_text").show()

        # ------------------------------------------------------------------
        # 4. Handle missing values (NULL)
        # ------------------------------------------------------------------

        # isNull() and isNotNull() are the right way to test for Spark NULL.
        employees.filter(F.col("salary").isNull()).show()
        employees.filter(F.col("manager").isNotNull()).show()

        # na.drop() can remove rows missing values in chosen columns.
        employees.na.drop(subset=["salary"]).show()

        # na.fill() replaces NULLs. Fill only salary here, leaving other columns
        # unchanged; the original employees DataFrame is not modified.
        employees.na.fill({"salary": 0}).show()

        # ------------------------------------------------------------------
        # 5. Aggregate data
        # ------------------------------------------------------------------

        # agg() calculates summary values. count() counts non-NULL salaries;
        # count("*") would count rows, including rows with NULL salaries.
        employees.agg(
            F.count("*").alias("employee_count"),
            F.count("salary").alias("employees_with_salary"),
            F.avg("salary").alias("average_salary"),
            F.max("salary").alias("highest_salary"),
        ).show()

        # groupBy() makes one group per department, then agg() summarizes each.
        # orderBy() sorts the resulting groups by average salary.
        department_summary = (
            employees.groupBy("department")
            .agg(
                F.count("*").alias("employee_count"),
                F.avg("salary").alias("average_salary"),
                F.sum("salary").alias("total_salary"),
            )
            .orderBy(F.col("department"))
        )
        department_summary.show()

        # distinct() removes duplicate rows. dropDuplicates([...]) compares only
        # the listed columns when deciding which rows are duplicates.
        employees.select("department").distinct().orderBy("department").show()

        # ------------------------------------------------------------------
        # 6. Join two DataFrames
        # ------------------------------------------------------------------

        # A join combines rows using matching key values. In real work these
        # DataFrames commonly come from separate tables or files.
        department_data = [
            {"department": "Engineering", "location": "Seattle"},
            {"department": "Sales", "location": "Chicago"},
            {"department": "Human Resources", "location": "New York"},
        ]
        departments = spark.createDataFrame(department_data)

        employee_locations = employees.join(
            departments,
            on="department",
            how="left",
        )
        employee_locations.select("name", "department", "location").show()

        # Common join types include "inner", "left", "right", and "full".
        # A left join keeps every row from the left DataFrame (employees).

        # ------------------------------------------------------------------
        # 7. Use SQL as another way to query a DataFrame
        # ------------------------------------------------------------------

        # A temporary view lets Spark SQL refer to a DataFrame by a name.
        employees.createOrReplaceTempView("employees")
        sql_result = spark.sql(
            """
            SELECT department, COUNT(*) AS employee_count,
                   AVG(salary) AS average_salary
            FROM employees
            GROUP BY department
            ORDER BY department
            """
        )
        sql_result.show()

        # ------------------------------------------------------------------
        # 8. Read and write files (examples; not run automatically)
        # ------------------------------------------------------------------

        # CSV input example:
        # csv_df = (
        #     spark.read
        #     .option("header", True)
        #     .option("inferSchema", True)
        #     .csv(r"data\employees.csv")
        # )
        #
        # For production data, prefer a known schema over inferSchema so Spark
        # does not need to scan data to guess types.
        #
        # Write CSV output example:
        # csv_df.write.mode("overwrite").option("header", True).csv(
        #     r"output\employees"
        # )
        #
        # Spark writes a folder of part files, not usually one single CSV file.
        # Common write modes: "error" (default), "overwrite", "append", "ignore".
        #
        # Other commonly used formats:
        # spark.read.parquet(r"data\employees")
        # some_df.write.mode("overwrite").parquet(r"output\employees")
        # Parquet is a common columnar format for analytics.

        # ------------------------------------------------------------------
        # 9. Important Spark concepts
        # ------------------------------------------------------------------

        # Transformations (select, filter, withColumn, join, groupBy) describe
        # work and return DataFrames. Spark generally evaluates them lazily.
        # Actions (show, count, collect, write) trigger execution.
        #
        # Avoid collect() on large DataFrames: it copies all returned data to
        # this computer's Python process. Use show() or limit() for inspection.
        #
        # DataFrames are immutable: each transformation returns a new DataFrame.
        # Keep the returned value if you want to use the transformed result.
        #
        # Spark distributes work across partitions. A DataFrame is not simply a
        # Python list, and row order is not guaranteed unless you orderBy().
        #
        # Built-in functions from pyspark.sql.functions (imported as F) are
        # usually preferable to Python UDFs because Spark can optimize them.

        # explain() prints Spark's execution plan, useful when learning how a
        # query will be carried out. It does not display the data itself.
        department_summary.explain()

    finally:
        # Always stop the local Spark session, even if an example raises an error.
        spark.stop()


if __name__ == "__main__":
    # This guard allows the file to be imported without running the tutorial.
    main()
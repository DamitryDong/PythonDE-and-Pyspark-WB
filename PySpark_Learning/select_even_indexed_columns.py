from pyspark.sql import SparkSession


def main() -> None:
    # A SparkSession is the starting point for working with Spark.
    # getOrCreate() reuses an existing session, or creates one if needed.
    spark = SparkSession.builder.appName("SelectEvenIndexedColumns").getOrCreate()

    # Make 100 column names: col_0, col_1, ..., col_99.
    # range(100) produces the integers 0 through 99 (the stop value is excluded).
    column_names = [f"col_{index}" for index in range(100)]

    # Make one sample row with the values 0 through 99.
    # Each value will go into the column with the same numbered suffix.
    row = tuple(range(100))

    # createDataFrame expects rows as data and a schema describing their columns.
    # The list around row makes it a collection containing one row.
    df = spark.createDataFrame([row], schema=column_names)

    # df.columns is the list of column names in their current order.
    # enumerate() pairs each name with its position, starting at index 0.
    # The modulo operator (%) gives the remainder after division:
    # an even index divided by 2 has a remainder of 0.
    # Therefore, this keeps positions 0, 2, 4, ..., 98 (50 columns).
    even_indexed_columns = []
    for index, column_name in enumerate(df.columns):
        if index % 2 == 0:
            even_indexed_columns.append(column_name)

    # select() takes column names as separate arguments.
    # The * unpacks the list so select receives each name individually.
    even_columns_df = df.select(*even_indexed_columns)

    # Show how many columns were selected and list their names.
    print(f"Selected {len(even_columns_df.columns)} columns:")
    print(even_columns_df.columns)

    # Display the resulting DataFrame rows in the console.
    even_columns_df.show()

    # Release Spark resources when the example is finished.
    spark.stop()


# This condition runs main() only when this file is executed directly.
# It avoids starting Spark if this file is imported by another Python file.
if __name__ == "__main__":
    main()

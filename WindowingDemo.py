from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    summary_df = spark.read.parquet("data/summary.parquet")
    running_total_window = Window.partitionBy("Country")\
        .orderBy("WeekNumber")\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("Running Total", f.sum("InvoiceValue").over(running_total_window))

    summary_df.show()
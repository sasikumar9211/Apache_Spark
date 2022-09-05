from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[3]")\
            .appName("SparkSQLTableDemo")\
            .enableHiveSupport()\
            .getOrCreate()

    flightTimeParquetDf = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDf.write\
                        .format("csv")\
                        .mode("overwrite")\
                        .bucketBy(5, "OP_CARRIER", "ORIGIN")\
                        .sortBy("OP_CARRIER", "ORIGIN")\
                        .saveAsTable("flight_data_tbl")

    print(spark.catalog.listTables("AIRLINE_DB"))




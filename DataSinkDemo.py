from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j


if __name__ == "__main__":
    spark =SparkSession.builder\
            .master("local[3]")\
            .appName("SparkDataSinkDemo")\
            .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDf = spark.read\
                          .format("parquet")\
                          .load("data/flight*.parquet")
    print("Num Partitions Before: "+str(flightTimeParquetDf.rdd.getNumPartitions()))
    flightTimeParquetDf.groupby(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDf.repartition(5)
    print("Num Partition After:"+str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupby(spark_partition_id()).count().show()

    partitionedDF.write\
        .format("csv")\
        .mode("overwrite")\
        .option("path", "datasink/avro/")\
        .save()

    flightTimeParquetDf.write\
            .format("json")\
            .mode("overwrite")\
            .option("path", "datasink/json/")\
            .partitionBy("OP_CARRIER", "ORIGIN")\
            .option("maxRecordPerFile", 10000)\
            .save()


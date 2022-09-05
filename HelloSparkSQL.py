import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, DateType, StructType, StringType, IntegerType

from lib.logger import Log4j

flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
])

flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    countDF.show()
    
    flightTimeCsv = spark.read\
                    .format("csv")\
                    .option("header", "true")\
                    .schema(flightSchemaStruct)\
                    .option("dateFormat","M/d/y")\
                    .load("data/flight*.csv")

    flightTimeCsv.show(5)
    print(flightTimeCsv.schema.simpleString())

    flightTimeJson = spark.read \
        .format("json") \
        .schema(flightSchemaDDL)\
        .load("data/flight*.json")

    flightTimeJson.show(5)
    print(flightTimeJson.schema.simpleString())

    flightTimeParq = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimeParq.show(5)
    print(flightTimeParq.schema.simpleString())


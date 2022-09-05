from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[3]")\
            .appName("LogFileDemo")\
            .getOrCreate()

    flie_df = spark.read.text("data/apache_logs.txt")
    flie_df.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logs_df = flie_df.select(regexp_extract("value", log_reg, 1).alias("IP"),
                             regexp_extract("value", log_reg, 4).alias("DATE"),
                             regexp_extract("value", log_reg, 6).alias("REQUEST"),
                             regexp_extract("value", log_reg, 10).alias("REFERRER"))

    logs_df\
        .where("trim(referrer) != '-' ")\
        .withColumn("referrer", substring_index("referrer", "/", 3))\
        .groupBy("referrer")\
        .count()\
        .show(100, truncate=False)



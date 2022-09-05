from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|m.l|ma"

    if re.search(female_pattern, gender.lower()):
        return "female"
    elif re.search(male_pattern, gender.lower()):
        return "male"
    else:
        return "Unknown"

if __name__ == "__main__":
        spark =SparkSession.builder.master("local[3]").appName("UDFDemo").getOrCreate()

        survey_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("data/survey.csv")

        survey_df.show(10)

        parse_gender_udf = udf(parse_gender, returnType=StringType())
        print("Catalog Entry:")
        [print(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
        survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
        survey_df2.show(10)

        spark.udf.register("parse_gender_udf", parse_gender, StringType())
        print("Catalog Entry:")
        [print(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
        survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
        survey_df3.show(10)
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import *

if __name__ == "__main__":
        spark =SparkSession.builder.master("local[3]").appName("UDFDemo").getOrCreate()

        invoice_df = spark.read\
                     .format("csv")\
                     .option("header", "true")\
                     .option("inferschema", "true")\
                     .load("data/invoices.csv")

        invoice_df.select(f.count("*").alias("Total_Count"),
                          f.sum("Quantity").alias("Total_Quantity"),
                          f.avg("UnitPrice").alias("Avg_Price"),
                          f.countDistinct("InvoiceNo").alias("CountDistinct")).show()


        invoice_df.selectExpr("count(1) as `Total_Count`",
                              "count(StockCode) as `Count Field`",
                              "sum(Quantity) as TotalQuantity",
                              "avg(UnitPrice) as AvgPrice").show()

        invoice_df.createOrReplaceTempView("sales")
        summary_sql = spark.sql("""
         Select Country,InvoiceNo, Sum(Quantity) as TotalQuantity,
          round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales 
          GROUP BY  Country, InvoiceNo""")

        summary_sql.show()

        summary_df = invoice_df\
                .groupby("Country", "InvoiceNo")\
                .agg(f.sum("Quantity").alias("TotalQuantity"),
                     f.round(f.sum(f.expr("Quantity *UnitPrice")), 2).alias("InvoiceValue"),
                     f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr")
                     )

        summary_df.show()


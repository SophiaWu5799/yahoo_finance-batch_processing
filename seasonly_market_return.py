# Databricks notebook source
#Read the Delta table from S3

s3_bucket = 'asc-de-training-destination-s3'
table_path = f"s3a://{s3_bucket}/de_1702/sophia_wu/yahoo_finance_project"

# from secrets improt S3_BUCKET
# from secrets improt TABLE_PATH

stock_df= spark.read.format("delta").load(f"{table_path}/stock_df")


# COMMAND ----------

# calculate each stock's the market return in each quarter of 2020 by using the closing prices in the market.
from pyspark.sql.functions import *

quarterly_avg_df = (stock_df.filter(year(col('date')) == '2020')
                            .withColumn('quarter', quarter('date'))
                            .groupBy('quarter','ticker','industry')
                            .agg(avg('close').alias('quarterly_avg_close'))
                            .orderBy('quarter',desc('quarterly_avg_close'))
                   )
display(quarterly_avg_df)




# COMMAND ----------

# compute the quarterly market return for each stock, and then group the stocks by industry. Within each industry group, assign a unique rank to each stock based on its quarterly market return, allowing us to determine the relative performance of each stock within its industry.
from pyspark.sql.window import Window

window_spec1 = Window.partitionBy( "ticker").orderBy("quarter")
window_spec2 = Window.partitionBy( "industry").orderBy("quarter",desc("quarterly_market_return"))

quarterly_market_return_df = (quarterly_avg_df
                    .select('quarter', 'ticker', (round((((col('quarterly_avg_close') / lag('quarterly_avg_close').over(window_spec1)) - 1) * 100),2).alias('quarterly_market_return')), 'industry')
                    .orderBy('quarter','industry')
                    .filter(col('quarter') != 1)
                    .withColumn('rank', rank().over(window_spec2))
                   )


display(quarterly_market_return_df)


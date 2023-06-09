# Databricks notebook source
# List tables in mysql database

JDBC_DRIVER = "com.mysql.jdbc.Driver"
db_name = 'de_001'
from secrets improt JDBC_URL, USER, PASSWORD, JDBC_DRIVER

table_list = (
    spark.read.format("jdbc")
    .option("driver", JDBC_DRIVER)
    .option("url", JDBC_URL)
    .option("dbtable", "information_schema.tables")
    .option("user", USER)
    .option("password", PASSWORD)
    .load()
    .filter(f"table_schema = '{db_name}'")
    .select("table_name")
)

table_list.show()



# COMMAND ----------

#read the covid table from mysql to databricks

covid_case_df = (
    spark.read.format("jdbc")
    .option("driver", JDBC_DRIVER)
    .option("url", JDBC_URL)
    .option("dbtable", "covidCase")
    .option("user", USER)
    .option("password", PASSWORD)
    .load()
)

display(covid_case_df)
covid_case_df.printSchema()

# COMMAND ----------

#change date format from 3/20/20 to 2023-03-20

from pyspark.sql.functions import *

covid_case_df = (covid_case_df.distinct()
                              .na.drop()
                              .withColumn('date_of_interest', date_format('date_of_interest','yyyy-MM-dd'))
                              .withColumn('date_of_interest', to_date('date_of_interest','yyyy-MM-dd'))
)

                   
display(covid_case_df)
covid_case_df.printSchema()


# COMMAND ----------

#Read the Delta table from S3

from secrets improt S3_BUCKET
from secrets improt TABLE_PATH

stock_table= spark.read.format("delta").load(f"{TABLE_PATH}/stock_df")



# COMMAND ----------


from secrets improt S3_PATH
SP500_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(S3_PATH)

SP500_df=SP500_df.withColumnRenamed('GICS?Sector', 'industry')


# COMMAND ----------

#correlation between COVID-19 case numbers and stock price in 2020

# A correlation coefficient of 1 indicates a perfect positive correlation, 
# while a value of -1 indicates a perfect negative correlation, 
# and a value of 0 indicates no correlation.
# In general, correlation coefficients above 0.7 or below -0.7 are often considered strong correlations.


# Quarterly 2 report
from pyspark.sql.functions import col, corr

# Join the two dataframes on the date column
quarter2_df =(stock_df.select('date', 'ticker', 'Close', 'industry')
                        .where(col('date').between('2020-03-01','2020-05-31'))
                        .join(broadcast(covid_case_df).select('date_of_interest', 'CASE_COUNT'),col('date')==col('date_of_interest'))
                        .drop('date_of_interest')
                        )

from pyspark.sql.functions import corr

# Compute the correlation between close price and case number
corr_q2_df = (quarter2_df.groupBy("ticker")
                        .agg(corr('Close', 'CASE_COUNT').alias('correlation'))
                        .withColumn('window', lit('Q2 2020'))
)

display(corr_q2_df)

#join the corr_df with sp500_df to caculate the impact of covid cases on different industry 

industry_q2_df = (corr_q2_df
                            .join(SP500_df, corr_q2_df.ticker == SP500_df.Symbol)
                            .groupBy('window','industry')
                            .agg(avg(col('correlation')).alias('avg_corr'))
                            .orderBy('avg_corr')
           
                   )
 
display(industry_q2_df)



# COMMAND ----------


#quarter 3 report

quarter3_df =(stock_df.select('date', 'ticker', 'Close', 'industry')
                        .where(col('date').between('2020-06-01','2020-08-31'))
                        .join(broadcast(covid_case_df).select('date_of_interest', 'CASE_COUNT'),col('date')==col('date_of_interest'))
                        .drop('date_of_interest')
                        )

# Compute the correlation between close price and covid case number
corr_q3_df = (quarter3_df.groupBy("ticker")
                        .agg(corr('Close', 'CASE_COUNT').alias('correlation'))
                        .withColumn('window', lit('Q3 2020'))
)

display(corr_q3_df)

industry_q3_df = (corr_q3_df
                            .join(SP500_df, corr_q3_df.ticker == SP500_df.Symbol)
                            .groupBy('window','industry')
                            .agg(avg(col('correlation')).alias('avg_corr'))
                            .orderBy('avg_corr')
           
                   )
 
display(industry_q3_df)



# COMMAND ----------

#quarter 4 report

quarter4_df =(stock_df.select('date', 'ticker', 'Close', 'industry')
                        .where(col('date').between('2020-10-01','2020-12-31'))
                        .join(broadcast(covid_case_df).select('date_of_interest', 'CASE_COUNT'),col('date')==col('date_of_interest'))
                        .drop('date_of_interest')
                        )

# Compute the correlation between close price and covid case number
corr_q4_df = (quarter4_df.groupBy("ticker")
                        .agg(corr('Close', 'CASE_COUNT').alias('correlation'))
                        .withColumn('window', lit('Q4 2020'))
)

display(corr_q4_df)


industry_q4_df = (corr_q4_df
                            .join(SP500_df, corr_q4_df.ticker == SP500_df.Symbol)
                            .groupBy('window','industry')
                            .agg(avg(col('correlation')).alias('avg_corr'))
                            .orderBy('avg_corr')
           
                   )
 
display(industry_q4_df)



# COMMAND ----------


from secrets import TABLE_PATH

industry_q2_df.write.format("delta").mode("overwrite").option("path", f"{TABLE_PATH}/stock_df").saveAsTable("industry_q2")
industry_q3_df.write.format("delta").mode("overwrite").option("path", f"{TABLE_PATH}/stock_df").saveAsTable("industry_q3")
industry_q4_df.write.format("delta").mode("overwrite").option("path", f"{TABLE_PATH}/stock_df").saveAsTable("industry_q4")

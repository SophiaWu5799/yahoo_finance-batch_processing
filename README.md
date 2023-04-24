
# The Pandemic’s Effect on the Stock Market: A Data engineering project

## Project Original Thoughts
Have you been confused by the impact of the COVID-19 pandemic on the stock market? The pandemic has caused significant volatility and uncertainty in the market, leaving many investors wondering how to navigate this new environment.

In this data engineering project, we aim to explore the effect of the COVID-19 pandemic on the stock market. By analyzing historical stock data and COVID case data, we hope to identify patterns and insights that can be used to inform investment decisions.

## Data Sources
This project uses the following data sources:

1. SP500 stock list (CSV file): The SP500 stock list is obtained from S3 bucket. The CSV file contains information about stock tickers, names, ectors and so on.

2. COVID case global data (CSV file): The COVID case global data is obtained from a MySQL database. The CSV file contains information about the number of COVID cases and deaths in different countries and regions.

3. Yahoo Finance API: The Yahoo Finance API is used to obtain the historical stock prices of the companies in the SP500 index. The API returns data such as the date, open, close, high, low, adjusted close, and volume.

## Data Ingestion and Storage
The data is extracted from the MySQL database and S3 bucket. The processed data is then stored in Auzer data factory.

## Data Pipeline
The data pipeline for this project involves the following steps:

1. Data ingestion: The data is extracted from S3 bucket and  MySQL database through JDBC.

2. Data cleaning, normalization, and transformation: 
 Data cleaning:
- Removing null or missing values by dropna() when download data from Yahoo finance API.
- Normalizing column names by withColumnRenamed(). For example, the spaces in column names need to be removed; otherwise, it can cause problems when accessing the column names in code or SQL queries. 
- Remove duplicate rows by dropDuplicates() or use groupBy() to group data by certain columns and aggregate the values
- Keep data type consisitency. Use cast() to convert data types to a consistent format, or regexp_extract() to extract specific patterns from string data

 Data normalization:
 
When downloading 500 stocks of 10 years' data to create a DataFrame, it can generate a large number of columns that may not be suitable for efficient queries and analysis. Therefore, it is recommended to expand the table vertically instead of horizontally. In this project, a standardized DataFrame format was created for each ticker symbol, and the resulting DataFrames were merged into a single DataFrame with consistent column names and data types. By vertically expanding the table in this way, the resulting df_all DataFrame can be easily queried and analyzed to extract insights about the historical stock prices of multiple companies. This approach allows for more efficient data processing and analysis, and can provide more meaningful insights into the performance of multiple stocks over time.

Data transformation:
  




3. Data storage: The processed data is stored in an S3 bucket.

4. Data visualization: Use Databricks' built-in SQL visualization functions to create charts, tables, and other visualizations based on the processed data.

## Technologies Used
The following technologies were used in this project:

1. Python: Python is used for data cleaning, transformation, and analysis.

2. Pandas: Pandas is used for data manipulation and analysis.

3. Spark: Spark is used for processing large amounts of data.

4. Databricks: Databricks is used as the computing platform for the project.

5. Tableau: Tableau is used for data visualization.

## Goal Achieved

The goal of this project is to compare the performance of different industries before, during, and after the pandemic. The project also aims to categorize companies into industries and compare the performance of companies in the same industry. The project uses historical, predictive, and forecasting analyses to determine the impact of the pandemic on the stock market. Information arbitrage is used to find meaning between unrelated information, such as comparing the number of COVID cases to the performance of a company's stock during a specific timeframe. The hypothesis is that the more COVID cases, the steeper the decline in the company's stock.

Overall, this data engineering project provides insights into the impact of the COVID-19 pandemic on the stock market and can be used by both data analysts (DA) and data scientists (DS) to inform investment decisions. The data is emphasized to be valuable for both groups, highlighting the versatility and applicability of the data in this project for a wide range of users.

## Conclusion
In conclusion, this data engineering project showcases the power of data analysis and visualization in understanding the impact of the COVID-19 pandemic on the stock market. By ingesting and transforming data from multiple sources, and using technologies such as Spark and Tableau, we were able to identify patterns and trends that can be used to inform investment decisions. The use of information arbitrage also allowed us to find meaning between seemingly unrelated information, further enriching our analysis. This project demonstrates the importance of data engineering in extracting insights from large and complex datasets, and the value of collaboration between data analysts and data scientists in leveraging these insights.

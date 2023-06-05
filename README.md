
# The Pandemic’s Effect on the Stock Market: A Data engineering project


## Project Original Thoughts
Have you been confused by the impact of the COVID-19 pandemic on the stock market? The pandemic has caused significant volatility and uncertainty in the market, leaving many investors wondering how to navigate this new environment.

In this data engineering project, we aim to explore the effect of the COVID-19 pandemic on the stock market. By analyzing historical stock data and COVID case data, we hope to identify patterns and insights that can be used to inform investment decisions.

![Example Image]（https://drive.google.com/file/d/1M4V64BBFdSOK8waHhtlKc4W_vJmOM6AP/view?usp=sharing）

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

2. Data cleaning
- Removing null or missing values by dropna() when download data from Yahoo finance API.
- Normalizing column names by withColumnRenamed(). For example, the spaces in column names need to be removed; otherwise, it can cause problems when accessing the column names in code or SQL queries. 
- Remove duplicate rows by dropDuplicates() or use groupBy() to group data by certain columns and aggregate the values
- Keep data type consisitency. Use cast() to convert data types to a consistent format, or regexp_extract() to extract specific patterns from string data

3. Data normalization
 
When downloading 500 stocks of 10 years' data to create a DataFrame, it can generate a large number of columns that may not be suitable for efficient queries and analysis. Therefore, it is recommended to expand the table vertically instead of horizontally. In this project, a standardized DataFrame format was created for each ticker symbol, and the resulting DataFrames were merged into a single DataFrame with consistent column names and data types. By vertically expanding the table in this way, the resulting df_all DataFrame can be easily queried and analyzed to extract insights about the historical stock prices of multiple companies. This approach allows for more efficient data processing and analysis, and can provide more meaningful insights into the performance of multiple stocks over time.

4. Data transformation:

- Refine the stock dataframe to contain only information regarding a particular stock during a specific time frame. For instance, filter the stock dataframe to show only data for Microsoft Inc. from January 2020 to December 2023.

- Compute the annual market return for a pool of 500 stocks between 2013 and 2023. For instance, estimate the percentage change in the value of the 500 stocks' composite index from the beginning of 2013 to the end of 2023. For example, the market returns of TSLA from 2014 to 2023 showed significant fluctuations. TSLA had an average market return of 22.28% from 2014 to 2019, with an increase of 68.45% in 2014, a small increase of 3.01% in 2015, and a decrease of 8.81% in 2016. In 2017, the market return surged to 49.84%, followed by a smaller increase of 0.95% in 2019. In 2020, TSLA experienced a market downturn of -13.8%, followed by a significant increase of 430.1% in 2021. However, the market return dropped to 168.97% in 2022 and decreased by -33.1% in 2023. 

- Merge the data from the stock price and stock information data sources, categorize the information by industry, and draw inferences on the comparative performance of various stock categories over a ten-year period. For instance, combine the data from the stock prices and the financial reports of various industries, group them by industry, and investigate how the technology industry has performed compared to the pharmaceutical industry from 2010 to 2020.

- Compute the quarterly market return for a pool of 500 stocks in 2020. For instance, when analyzing the correlation coefficient for AAPL, we observed a shift from a strong negative correlation in Q2 2020 (-0.681) and Q3 2020 (-0.680) to a strong positive correlation in Q4 2020 (0.804). This analysis provides insights into how the relationship between COVID cases and stock prices changed over time, not just for AAPL but also for other companies.

- Summarize the data by industry to compare the performance of various industries on a quarterly basis in 2020. For instance, aggregate the data by sector and examine how the consumer goods industry fared compared to the energy sector in Q2 2020.

- Combine the stock dataframe with the COVID case dataframe to investigate the correlation between stock performance and COVID case numbers. For example, merge the data on a date basis, group the data by the number of COVID cases, and examine how the stock prices of companies in the healthcare sector correlated with the number of COVID cases from January 2020 to December 2021.

5. Data storage: The processed data is stored in the S3 bucket.

6. Data visualization: Use Databricks' built-in SQL visualization functions to create charts, tables, and other visualizations based on the processed data.


## Technologies Used
The following technologies were used in this project:

1. Spark: Spark is used for processing large amounts of data.
2. Python: Python is used for data cleaning, transformation, and analysis.
3. Pandas: Pandas is used for data manipulation and analysis.
4. Databricks: Databricks is used as the computing platform and visualization tool for the project.

## Goal Achieved

This data engineering project involved processing and analyzing stock market data to gain insights into the performance of different stocks, sectors, and industries before, during, and after the pandemic. The primary goals of the project were achieved, including filtering and refining the stock data by stock and time frame, computing annual and quarterly market returns, merging different datasets, and exploring correlations between stock performance and external factors such as COVID-19 cases.


## Conclusion
In conclusion, this project showcased the power of data analysis and visualization in understanding the impact of the COVID-19 pandemic on the stock market. By ingesting and transforming data from multiple sources, and using technologies such as Spark and python, I was able to identify patterns and trends that can be used to inform investment decisions. The use of information arbitrage also allowed us to find meaning between seemingly unrelated information, further enriching our analysis. This project demonstrates the importance of data engineering in extracting insights from large and complex datasets, and the value of collaboration between data analysts and data scientists in leveraging these insights.



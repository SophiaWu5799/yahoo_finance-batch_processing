# yahoo_finance-batch_processing

## The Pandemic’s Effect on the Stock Market: A Data-Driven Study

## Project Original Thoughts
Have you been confused by the impact of the COVID-19 pandemic on the stock market? The pandemic has caused significant volatility and uncertainty in the market, leaving many investors wondering how to navigate this new environment.

In this data engineering project, we aim to explore the effect of the COVID-19 pandemic on the stock market. By analyzing historical stock data and COVID case data, we hope to identify patterns and insights that can be used to inform investment decisions.

## Data Sources
This project uses the following data sources:

1. SP500 stock list (CSV file): The SP500 stock list is obtained from a MySQL database. The CSV file contains information about the stock ticker, name, and sector of each company in the index.

2. COVID case global data (CSV file): The COVID case global data is obtained from an Azure storage account. The CSV file contains information about the number of COVID cases and deaths in different countries and regions.

3. Yahoo Finance API: The Yahoo Finance API is used to obtain the historical stock prices of the companies in the SP500 index. The API returns data such as the date, open, close, high, low, adjusted close, and volume.

##Data Ingestion and Storage
The data is extracted from the MySQL database and Azure storage account using pandas and Python. The processed data is then stored in an S3 bucket.

## Data Pipeline
The data pipeline for this project involves the following steps:

1. Data ingestion: The data is extracted from the MySQL database and Azure storage account using pandas and Python.

2. Data cleaning and transformation: The data is cleaned and transformed to remove any missing or invalid values. The data is then categorized into industries based on the sector of each company. The stock prices are also normalized to account for any changes in the value of the US dollar during the pandemic.

3. Data storage: The processed data is stored in an S3 bucket.

4. Data processing: The data is processed using Spark to perform large-scale data analysis.

5. Data visualization: The results of the data analysis are visualized using Tableau. The visualization helps to highlight trends and patterns in the data, making it easier to understand.

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

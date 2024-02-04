GitHub Project Description - Spark: Data Loading, Transformation, and Feature Engineering with PySpark

Project Overview:
The "Spark" GitHub project focuses on leveraging PySpark for the efficient loading and transformation of real-world data. Specifically, the project delves into the realm of car sales data in the UK. The primary objectives are to showcase the power of PySpark in handling large datasets, perform data transformations, and generate new features for deeper analytical insights, and load the result in Google Cloud.

Key Features:

PySpark Data Extract:
  Utilizes PySpark to load a substantial dataset of UK car sales into a distributed computing environment.
Demonstrates the scalability and efficiency of PySpark for handling big data.

Data Transformation:
  Implements various data transformation tasks using PySpark's DataFrame API.
Addresses common data preparation challenges, such as handling missing values, encoding categorical features, and standardizing data types.

Feature Engineering:
  Introduces feature engineering techniques to create new relevant features from existing data.
Illustrates how to extract valuable information from the dataset to enhance the analysis.

PySpark Data Loading:
  With PySpark we make a connection with Google Cloud, using buckets y bigQuery to load the results from the previous transformations. With the data I will do ML using bigQuery ML.

Jupyter Notebook Analysis:
  Includes a Jupyter Notebook that serves as an interactive platform for preliminary data analysis.
Provides a step-by-step walkthrough of the data loading, transformation, and feature engineering process.
Real-World Car Sales Data:

The project utilizes a dataset containing actual information on car sales in the UK, adding authenticity to the analysis.
Demonstrates the practical application of PySpark in handling and analyzing real-world business data.

Notes:
  The current project involves setting up a local instance of Spark to connect with Google Cloud Platform (GCP), specifically to interact with BigQuery and Google Cloud Storage (GCS). However, we have encountered significant challenges due to the absence of two essential connectors: spark-bigquery and gcs-connector. This connectors are already in Dataproc and there are not intended for use with a local instalation of Spark.

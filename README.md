# data-engineering-portfolio

# Azure ecommerce C2C data-pipeline project
- **Brief overview**: In the fast-paced world of e-commerce, having timely and accurate data is critical to driving business decisions. This project focuses on designing, implementing, and optimizing data pipelines to streamline the collection, processing, and analysis of large-scale e-commerce data. The goal is to provide the business with actionable insights on user behavior, market dynamics, and platform engagement across different countries. To aim to enhance user engagement, and seller-buyer relationships. <br>
- **Technology used**: Azure Data Factory, Azure Data Lake Storage, Azure Databricks, AWS S3
- **Outcome**:
  - Implementation: Build data factory pipeline, create a lakehouse by using the idea of the medallion architecture design pattern using Bronze(raw data), Silver(cleanse and normalize the data) and Gold layers (combine all table into one big table) that is enabled via Delta Lake. <br>
<span><img src="Azure_ecommerce_data_project/ecom_fashion_store_architecture.png" alt="drawing" width="400"/></span>

# AWS spotify data-pipeline project
- **Brief overview**: <br>
- **Technology used**: AWS Lambda, AWS Glue notebook, AWS S3, Snowpipe, Snowflake
- **Outcome**:
  - Implementation:  <br>
<span><img src="AWS_spotify_spark_data_pipeline/spotify_achitecture.png" alt="drawing" width="400"/></span>


# GCP uber data-pipeline project
- **Brief overview**: This project is a simulation of the process of data engineering from getting the source file until the data analytic stage. <br>
- **Technology used**: Google Cloud Platform - Cloud Storage, Compute Engine, Mage AI, Big Query, Looker Studio <br>
- **Outcome**: 
  - Plan stage: describe the technology used and design the data model for analytic usage
  - Implementation stage: carry out a data transformation and build a data pipeline, exporting transformed data into a data platform
  - Data Analytic stage: Perform data analysis, and present the dashboard. <br>
<span><img src="GCP_Uber_data_pipeline/architecture-gcp.png" alt="drawing" width="400"/></span>

# Apache Airflow meowfact project
- **Brief overview**: This project is a simple project of using apache airflow to control the series of tasks. <br>
- **Technology used**: Google Compute Engines, Apache Airflow, AWS S3 <br>
- **Outcome**:
  - Plan stage: plan for the technology used.
  - Implementation: Extract data from API, write into a file, and uplaod the file in S3, using Apache Airflow Scheduler

# Kafka HM product project
- **Brief overview**: This project is simulation of usage of Kafka and utilising set of cloud services tools provided by Google.<br>
- **Technology used**: Kafka, Google Cloud Platform - Cloud Storage, Compute Engine, Big Query<br>
- **Outcome**:
  - Plan stage: Design the data pipeline and data modeling the data.
  - Implementation stage: Get the datasource from kaggle, study the dataset, perform the data cleansing, create and setup instance on google compute engine, pushing file to s3 and build BigQuery table from Cloud Storage.
    <br><br>
<span><img src="Kafka-h&m-product/architecture_h&m_product.png" alt="drawing" width="400"/></span> <br>
**Enhancement still pending to make: Automate the whole flow <br>


# Small Projects
- Data Cleaning using Pandas and Spark.

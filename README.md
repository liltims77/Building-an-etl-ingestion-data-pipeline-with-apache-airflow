# Building-and-ingesting-data-through-pipeline
This project involves building an ETL pipeline for a data set transforming data into scripts, storing and querying in postgres database (Pgcli and Pgadmin). Ingested data is then dockerized and Apache Airflow is used to monitor data workflow in AWS cloud storage.

# Project Description
This project involves building pipeline for a data sets. The data sets (Ny_taxi) was gotten from[ny taxi page] (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). A python pipeline was first built and jupyter notebook was used to insert data into postgres database (pgcli) . Another database was created (pgadmin) and merged with pgcli to run and query the data set more efficently and effectively using postgresql. The data set was then changed to parquet ingested Chunk by chunk and dockerized (ingesting data into docker container). Apache airflow was used to schedule the workflow of the dataset from download, change to parquet, injesting the DAG inot AWS S3 bucket and running the airflow directly on Amanzon Managed Workflow for Apache airflow (MWAA) services.


# Airflow
A S3 bucket was created first and the DAG and requirements.txt folder was put inside the bucket.
Amanzon Managed Workflow for Apache airflow  was also created and buckets from S3 was uploaded into the MWAA.

The airflow graph view 
![Screenshot 2022-07-13 234210](https://user-images.githubusercontent.com/41475769/179023337-8f98f643-a841-4975-8eb4-21dda4f3fc89.png)

![Screenshot 2022-07-13 234808](https://user-images.githubusercontent.com/41475769/179023421-d716c18e-3a8a-4de4-bde3-f0348cf921cf.png)

The airflow tree view
![Screenshot 2022-07-13 234626](https://user-images.githubusercontent.com/41475769/179023639-f1f90fb3-f3e7-448b-8080-3c6be2361c4d.png)
the airflow dag rund daily job to the etl pipeline.


# Problem Statement
[This dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) provides information of orders made at [Olist Store](https://olist.com/pt-br/), a Brazilian Ecommerce, from 2016 to 2018. 

In this project, we will build a data pipeline to ingest and process the data, and eventually visualise the data in Google Bigquery.
find out the states with the most number of orders made and the daily volume of orders from these states.


# Dashboard
https://datastudio.google.com/reporting/216f796f-e212-4d7f-bdbb-6ff6a5877761
[Dashboard](./images/dashboard.png)


# Running the project
- Set your bigquery project in the following

Airflow
```bash
docker-compose up
```


# Data ingestion by DAG
ingest_olist_dag
gcs_to_bq_dag
bq_transformation_dag


# Technologies used
- Terraform
- Docker
- Airflow
- Google Cloud Storage
- Google Bigquery
- Google Data Studio


# Tables
We have used Airflow to orchestrate the ingestion of data to GCS and moving the data to Google Bigquery.


# Acknowledgement
I am very grateful to the following instructors who have spent a great amount of time and effort ti put together the materials at [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
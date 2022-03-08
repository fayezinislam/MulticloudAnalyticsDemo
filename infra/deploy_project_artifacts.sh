#!/bin/bash

##
# BEGIN FUNCTION DECLARATION
##

echo "Enter GCP project id for demo:"
read project

#SET PROJECT
gcloud config set project $project

function createBigQueryDatasets {
    echo "Creating BigQuery datasets..."

    #CREATE AWS STAGING DATASET
    bq --location=aws-us-east-1 mk \
    --dataset \
    $project:bq_omni_staging_s3
    
    #CREATE SNOWFLAKE STAGING DATASET
    bq --location=us-central1 mk \
    --dataset \
    $project:snowflake
    
    #CREATE MYSQL STAGING DATASETS
    bq --location=us-central1 mk \
    --dataset \
    $project:mysql_dataset_final

    bq --location=us-central1 mk \
    --dataset \
    $project:mysql_dataset_log
    
    #CREATE US EIA STAGING DATASETS
    bq --location=us-central1 mk \
    --dataset \
    $project:us_eia
}

function createBigQueryTables {
    echo "Creating BigQuery tables..."
    
    #CREATE SNOWFLAKE STAGING TABLE
    bq mk --table snowflake.TEXAS_WEATHER_HISTORICAL_TEST \
    STATION:STRING,DATE:DATE,TAVG:FLOAT,TMIN:FLOAT,TMAX:FLOAT,PRCP:FLOAT,SNOW:FLOAT,WDIR:STRING,WSPD:FLOAT,WPGT:FLOAT,PRES:FLOAT,TSUN:FLOAT
    
    echo "Tables for the following datasets need to be created using AI platform notebooks: us_eia"
    echo "Tables for the following datasets will automatically be created using Dataflow jobs: mysql_dataset_final, mysql_dataset_log"
    echo "External tables for BQ omni needs to be created by following intructions here: https://docs.google.com/document/d/1CSQy3zqC_wrBxcFsRPFhPZOiTzBs5yY_IZY4owECsM0"

}

function createMySqlDatabase {
    echo "Creating MySQL database..."
    
    #CREATE MYSQL DATABASE INSTANCE
    gcloud sql instances create fsi-select-mysql \
    --database-version=MYSQL_5_7 \
    --cpu=4 \
    --memory=26624MB \
    --region=us-central1
    
    echo "Dont forget to set the root password using the command: gcloud sql users set-password root"
}

function createDataflowJobs {
    echo "Creating Dataflow job for snowflake import"
    echo "Enter Dataflow GCS staging location:"
    read staging
    echo "Enter JDBC driver GCS staging location:"
    read drivers
    echo "Enter BigQuery GCS staging location:"
    read bqtemp
        
    #CREATE SNOWFLAKE IMPORT DATAFLOW JOB
    gcloud dataflow jobs run snowflake-to-bq-import-2035 \
    --gcs-location gs://dataflow-templates-us-central1/latest/Jdbc_to_BigQuery \
    --region us-central1 \
    --num-workers 2 \
    --staging-location $staging \
    --parameters connectionURL=jdbc:snowflake://ze65011.us-central1.gcp.snowflakecomputing.com:443/?account=ze65011.us-central1.gcp&warehouse=COMPUTE_WH&db=GCP_DEMO_DB&schema=DEMO,driverClassName=net.snowflake.client.jdbc.SnowflakeDriver,query=SELECT * FROM "GCP_DEMO_DB"."DEMO"."TEXAS_WEATHER_HISTORICAL" ORDER BY DATE,outputTable=$project:snowflake.TEXAS_WEATHER_HISTORICAL,driverJars=$drivers,bigQueryLoadingTemporaryDirectory=$bqtemp,username=dataflowuser,password=demopassword

}

##
# END FUNCTION DECLARATION
##

##
# BEGIN SCRIPT EXECUTION
##

createBigQueryDatasets
createBigQueryTables
createMySqlDatabase
createDataflowJobs
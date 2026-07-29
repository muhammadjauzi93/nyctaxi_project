# Databricks notebook source
# Import libraries
import urllib.request
import shutil
import os

# COMMAND ----------

# For one file
#url ='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet'

#response = urllib.request.urlopen(url)

#dir_path ="/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2026-01"

#os.makedirs(dir_path, exist_ok=True)


#local_path = dir_path + "/yellow_tripdata_2026-01.parquet"

#with open(local_path, 'wb') as f:
#    shutil.copyfileobj(response,f)


# Automate for multiple files
years_to_process = ['2026']
months_to_process = ['01','02','03','04']

for year in years_to_process:
  for month in months_to_process:
    url =f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
    response = urllib.request.urlopen(url)
    dir_path =f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{year}-{month}"

    os.makedirs(dir_path, exist_ok=True)

    local_path = f"{dir_path}/yellow_tripdata_{year}-{month}.parquet"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response,f)



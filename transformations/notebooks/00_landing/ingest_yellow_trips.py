# Import libraries
import urllib.request
import shutil
import os
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime('%Y-%m')

# Define the local directory for this date's data
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# Define the full path for the downloaded file
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try:
    # Check if the file already exists
    dbutils.fs.ls(local_path)

    # If the file already exits then set continue_downstream to no
    dbutils.jobs.taskValues.set("continue_downstream", "no")
    print("File already downloaded, aborting donwstream task.")

except:
    try:
        # Construct the download URL for the required parquet file
        url =f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet'

        # Open a connection and stream the remote file
        response = urllib.request.urlopen(url)
        
        # Create the directory for this date's data
        os.makedirs(dir_path, exist_ok=True)

        # Save the downloaded file to the local path
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response,f)

        # Set continue_downstream to yes if the file was downloaded successfully
        dbutils.jobs.taskValues.set("continue_downstream", "yes")
        print("File successfully uploaded in the current run.")

    except Exception as e:
        # Set continue_downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set("continue_downstream", "no")
        print(f"Error downloading file: {str(e)}")


    

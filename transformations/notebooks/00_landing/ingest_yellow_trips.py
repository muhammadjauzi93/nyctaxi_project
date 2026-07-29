import sys
import os

# Go up two directories to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))

if project_root not in sys.path:
    sys.path.append(project_root)

# Import libraries
import urllib.request
import shutil
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file


# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyymm(2)

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

        # Download the file from the URL and save it to the specified local path
        download_file(url, dir_path, local_path)

        # Set continue_downstream to yes if the file was downloaded successfully
        dbutils.jobs.taskValues.set("continue_downstream", "yes")
        print("File successfully uploaded in the current run.")

    except Exception as e:
        # Set continue_downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set("continue_downstream", "no")
        print(f"Error downloading file: {str(e)}")


    

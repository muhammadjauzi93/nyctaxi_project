import sys
import os

# Go up two directories to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))

if project_root not in sys.path:
    sys.path.append(project_root)

# Import libraries
import urllib.request
import shutil
from modules.data_loader.file_downloader import download_file

try:
    # Define the URL for the CSV file
    url ='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    # Set the directory path for saving the file
    dir_path =f"/Volumes/nyctaxi/00_landing/data_sources/lookup"

    # Set the local file path for the CSV
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Download and save the CSV file locally
    download_file(url, dir_path, local_path)

    dbutils.jobs.taskValues.set("continue_downstream", "yes")
    print("File successfully uploaded")
except Exception as e:
    dbutils.jobs.taskValues.set("continue_downstream", "no")
    print(f"File download failed: {str(e)}")
# Import libraries
import urllib.request
import shutil
import os

try:
    # Define the URL for the CSV file
    url ='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    # Open the URL and get the response object
    response = urllib.request.urlopen(url)

    # Set the directory path for saving the file
    dir_path =f"/Volumes/nyctaxi/00_landing/data_sources/lookup"
    os.makedirs(dir_path, exist_ok=True)  # Create directory if it doesn't exist

    # Set the local file path for the CSV
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Download and save the CSV file locally
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response,f)

    dbutils.jobs.taskValues.set("continue_downstream", "yes")
    print("File successfully uploaded")
except Exception as e:
    dbutils.jobs.taskValues.set("continue_downstream", "no")
    print(f"File download failed: {str(e)}")
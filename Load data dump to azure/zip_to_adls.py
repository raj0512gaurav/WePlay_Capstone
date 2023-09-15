# Databricks notebook source
# MAGIC %md
# MAGIC ##Extracting zip file to ADLS using url

# COMMAND ----------

from zipfile import ZipFile
import requests
import io

# COMMAND ----------

# URL of the ZIP file
zip_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/89489b5b-04a5-4834-ab3e-20253bdb3238_83d04ac6-cb74-4a96-a06a-e0d5442aa126_ipl_data.zip"

# Define the target directory in ADLS
target_dir = "/dbfs/mnt/weplaybatchraw/"

# Download the ZIP file
response = requests.get(zip_url)

# COMMAND ----------

# Check if the download was successful
if response.status_code == 200:
    # Extract the ZIP file
    with ZipFile(io.BytesIO(response.content)) as zip_ref:
        zip_ref.extractall(target_dir)
        print("ZIP file extracted successfully.")
else:
    print("Failed to download ZIP file.")

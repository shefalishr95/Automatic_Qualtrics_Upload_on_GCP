# Automatic_Qualtrics_Upload_GCP

**Automate upload of survey results from Qualtrics to BigQuery**

This repository was created to link Qualtrics and BigQuery such that survey data is cleaned, transformed and uploaded on Google cloud at a pre-specified frequency at minimal cost. The steps followed are as follows:

1. Publish a message to Pub/Sub
2. Define Cloud Function (linked to Pub/Sub) with Python script for data transformation and upload
3. Schedule a CRON job to run cloud function at specified frequency
4. Check BigQuery dataset to ensure successful run

The work is inspired by @rafaello9472's [work](https://github.com/rafaello9472/c4ds). 

This repository contains the Python script, linked to a Cloud Function, which extracted data from Qualtrics API, transformed it as per requirements, and loaded the dataset to BigQuery. The project folder is organized as follows:

```
proj/
├── README.md
├── cloud-function-script   
│    ├── requirements.txt
│    ├── main.py
```

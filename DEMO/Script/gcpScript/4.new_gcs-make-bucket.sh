#!/bin/bash
GCP_PROJECT_NAME=PROJECT-NAME	# <---- Inserire CUSTOMPROJECTNAME
GCS_BUCKET_NAME=${GCP_PROJECT_NAME}-file-bucket
GCS_BUCKET_ZONE=europe-west1
GCS_BUCKET_CLASS=regional
GCP_PROJECT_ID=scalaproject-230113
FILE_FOLDER_PATH=/home/anto/Scrivania/GUI/Script/gcpScript/files

gsutil mb -c ${GCS_BUCKET_CLASS} -l ${GCS_BUCKET_ZONE} -p ${GCP_PROJECT_ID} gs://${GCS_BUCKET_NAME} && echo "#:Bucket ${GCS_BUCKET_NAME} creato" && gsutil -m cp -r ${FILE_FOLDER_PATH} gs://${GCS_BUCKET_NAME} && echo "#:FILES UPLOADED" 


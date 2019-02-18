#!/bin/bash
GCP_PROJECT_NAME=scala-project
GCS_BUCKET_NAME=${GCP_PROJECT_NAME}-file-bucket
GCS_BUCKET_OUTPUT_FOLDER=${GCS_BUCKET_NAME}/files/output

DOWNLOAD_DIRECTORY=/home/anto/Scrivania/GUI/public

gsutil cp gs://${GCS_BUCKET_OUTPUT_FOLDER}/part-* ${DOWNLOAD_DIRECTORY} && cat ${DOWNLOAD_DIRECTORY}/part-* > ${DOWNLOAD_DIRECTORY}/result.txt echo  "URL_DOWNLOAD:result.txt"

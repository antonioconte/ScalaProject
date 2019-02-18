#!/bin/bash
DATAPROC_WORKER_NUM=$1
DATAPROC_CORE_NUM=$2
GCP_PROJECT_ID=scalaproject-230113
DATAPROC_CLUSTER_NAME=scala-project-cluster-1master-${DATAPROC_WORKER_NUM}worker-${DATAPROC_CORE_NUM}core
DATAPROC_CLUSTER_REGION=europe-west1

gcloud dataproc clusters delete -q ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} --project ${GCP_PROJECT_ID} && echo "DONE!"

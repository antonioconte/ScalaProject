#!/bin/bash
GCP_PROJECT_NAME=scala-project
GCP_PROJECT_ID=scalaproject-230113
GCS_BUCKET_NAME=${GCP_PROJECT_NAME}-file-bucket

DATAPROC_CORE_NUM=$2

DATAPROC_MASTER_NUM=1
DATAPROC_MASTER_DISK_TIPE=pd-standard
DATAPROC_MASTER_DISK_SIZE=16GB
DATAPROC_MASTER_MACHINE_TYPE=n1-standard-${DATAPROC_CORE_NUM}

DATAPROC_WORKER_NUM=$1
DATAPROC_WORKER_DISK_TIPE=pd-standard
DATAPROC_WORKER_DISK_SIZE=16GB
DATAPROC_WORKER_MACHINE_TYPE=n1-standard-${DATAPROC_CORE_NUM}


DATAPROC_CLUSTER_NAME=scala-project-cluster-${DATAPROC_MASTER_NUM}master-${DATAPROC_WORKER_NUM}worker-${DATAPROC_CORE_NUM}core
DATAPROC_CLUSTER_REGION=europe-west1
DATAPROC_CLUSTER_ZONE=europe-west1-d

gcloud dataproc clusters create ${DATAPROC_CLUSTER_NAME} \
	--region ${DATAPROC_CLUSTER_REGION} \
	--zone ${DATAPROC_CLUSTER_ZONE} \
	--scopes storage-rw \
	--project ${GCP_PROJECT_ID} \
	--bucket ${GCS_BUCKET_NAME} \
	--num-masters ${DATAPROC_MASTER_NUM} \
	--master-boot-disk-type ${DATAPROC_MASTER_DISK_TIPE} \
	--master-boot-disk-size ${DATAPROC_MASTER_DISK_SIZE} \
	--master-machine-type ${DATAPROC_MASTER_MACHINE_TYPE} \
	--num-workers ${DATAPROC_WORKER_NUM} \
	--worker-boot-disk-type ${DATAPROC_WORKER_DISK_TIPE} \
	--worker-boot-disk-size ${DATAPROC_WORKER_DISK_SIZE} \
	--worker-machine-type ${DATAPROC_WORKER_MACHINE_TYPE} && echo "#:Cluster created!"


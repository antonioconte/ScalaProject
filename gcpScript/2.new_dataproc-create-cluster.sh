#!/bin/bash

. 0.new_variables.sh

./90.log-time.sh "STARTING CLUSTER '${DATAPROC_CLUSTER_NAME}' ..."
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
	--worker-machine-type ${DATAPROC_WORKER_MACHINE_TYPE} 
./90.log-time.sh "CLUSTER '${DATAPROC_CLUSTER_NAME}' STARTED!"

#!/bin/bash

. 0.new_variables.sh

./90.log-time.sh "DELETING CLUSTER '${DATAPROC_CLUSTER_NAME}' ..."
gcloud dataproc clusters delete -q ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} --project ${GCP_PROJECT_ID}
./90.log-time.sh "CLUSTER '${DATAPROC_CLUSTER_NAME}' DELETED!"

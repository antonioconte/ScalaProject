#!/bin/bash

. 0.new_variables.sh
./90.log-time.sh "CREATING BUCKET '${GCS_BUCKET_NAME}' ..."
gsutil mb -c ${GCS_BUCKET_CLASS} -l ${GCS_BUCKET_ZONE} -p ${GCP_PROJECT_ID} gs://${GCS_BUCKET_NAME}
#gsutil mb -c ${GCS_SRC_BUCKET_CLASS} -l ${GCS_SRC_BUCKET_ZONE} -p ${GCP_PROJECT_ID} gs://${GCS_SRC_BUCKET_NAME}
./90.log-time.sh "BUCKET '${DATAPROC_CLUSTER_NAME}' CREATED!"

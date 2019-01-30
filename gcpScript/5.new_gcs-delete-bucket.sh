#!/bin/bash

. 0.new_variables.sh

./90.log-time.sh "DELETING BUCKET '${GCS_BUCKET_NAME}' ..."
gsutil rm -r gs://${GCS_BUCKET_NAME}
#gsutil rm -r gs://${GCS_SRC_BUCKET_NAME}
./90.log-time.sh "BUCKET '${DATAPROC_CLUSTER_NAME}' DELETED!"

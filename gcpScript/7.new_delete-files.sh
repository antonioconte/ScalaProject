#!/bin/bash

. 0.new_variables.sh

./90.log-time.sh "DELETING FILE '${DIR_OUTPUT}' ..."
gsutil rm -r ${DIR_OUTPUT}
#gsutil rm -r gs://${GCS_SRC_BUCKET_NAME}
./90.log-time.sh "FILE '${DIR_OUTPUT}' DELETED!"

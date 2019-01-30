#!/bin/bash

. 0.new_variables.sh

gsutil cp ${SCALA_JAR_FILE} gs://${GCS_BUCKET_NAME}/src/${SCALA_JAR_FILENAME}
#gsutil cp ${DATA_FILE} gs://${GCS_DATA_BUCKET_NAME}/



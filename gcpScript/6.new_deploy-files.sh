#!/bin/bash

. 0.new_variables.sh

#gsutil cp ${SCALA_JAR_FILE} gs://${GCS_BUCKET_SRC_FOLDER}/${SCALA_JAR_FILENAME}
#gsutil cp ${DATA_FILE} gs://${GCS_BUCKET_DATA_FOLDER}/${DATA_FILENAME}
#gsutil cp -r ${OUTPUT_FOLDER} gs://${GCS_BUCKET_OUTPUT_FOLDER}

gsutil cp -r ${FILE_FOLDER_PATH} gs://${GCS_BUCKET_NAME}




#!/bin/bash

. 0.new_variables.sh

./90.log-time.sh "RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' OVER '${DATA_FILE}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
gcloud dataproc jobs submit spark --id ${JOB_ID} --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      --project ${GCP_PROJECT_ID} \
      -- ${TYPE_COMP} ${DEMO} ${FILE_INPUT} ${DIR_OUTPUT} ${LAMBDA} ${ITER} ${PART} ${TIMEOUT}
./90.log-time.sh "SPARK JOB '${SCALA_RUNNABLE_CLASS}' DONE!"


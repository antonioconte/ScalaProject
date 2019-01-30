#!/bin/bash

. 0.new_variables.sh


#./4.new_gcs-make-bucket.sh
#./2.new_dataproc-create-cluster.sh 



esc=false

while ! $esc 
do
	echo "Press"
	echo "[1] to create cluster"
	echo "[2] to upload file"
	echo "[3] to run a job"
	echo "[4] to delete cluster"

	read var_enter
	if [ ${var_enter} == "1" ] #create cluster
	then
		./4.new_gcs-make-bucket.sh
		./2.new_dataproc-create-cluster.sh
	else
		if [ ${var_enter} == "2" ] #upload file
		then
			echo "upload"
			./6.new_deploy-files.sh
		else
			if [ ${var_enter} == "3" ] #run a job
			then
				echo "run"
			else
				if [ ${var_enter} == "4" ] #delete cluster
				then
					./3.new_dataproc-delete-cluster.sh
					./5.new_gcs-delete-bucket.sh
				else
					echo "Invalid Input, error, aborting"
					esc=true
				fi
			fi
		fi
	fi
done

#./03.deploy-code.sh
#./04.dataproc-run-job.sh
#./05.dataproc-delete-cluster.sh

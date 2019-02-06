#!/bin/bash

. 0.new_variables.sh


#./4.new_gcs-make-bucket.sh
#./2.new_dataproc-create-cluster.sh 



esc=false
firstTimeCreated=true
firstTimeDeleted=true

while ! $esc 
do
	echo "Press"
	echo "[1] to create bucket and cluster"
	echo "[2] to upload files"
	echo "[3] to run a job"
	echo "[4] to delete bucket and cluster"
    echo "any other key to exit"


	read var_enter
	if [ ${var_enter} == "1" ] #create bucket and cluster
	then
	    if $firstTimeCreated
	    then
		    ./4.new_gcs-make-bucket.sh
		    ./2.new_dataproc-create-cluster.sh
		    firstTimeCreated=false
        else
            echo "già avviati"
        fi
	else
		if [ ${var_enter} == "2" ] #upload files
		then
			echo "upload"
			./6.new_deploy-files.sh
		else
			if [ ${var_enter} == "3" ] #run a job
			then
				echo "run"
				8.new_dataproc-run-job.sh
			else
				if [ ${var_enter} == "4" ] #delete bucket and cluster
				then
                    if $firstTimeDeleted && ! $firstTimeCreated
                    then
                        ./3.new_dataproc-delete-cluster.sh
					    ./5.new_gcs-delete-bucket.sh
                        firstTimeDeleted=false
                    else
                        echo "non esistenti"
                    fi
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

#!/bin/sh
################################################################################
# Program      : gen_dq_config_load.sh
# Date Created : 10/06/2015
# Description  :
# Parameters   :  <ENV NAME>
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 04/07/2016   Sanjeeb Panda                Creation
################################################################################

##############################################################
# INITIALIZE JOB VARIABLES
##############################################################

JOB_START_TIME=$(date "+%Y%m%d%H%M%S")
typeset Job_Name=$(basename $0)
typeset Dir_Name=$(dirname $0)
typeset Start_Time=$(date)

##############################################################
# TEST NUMBER OF PARAMETERS & ODATE TYPE
##############################################################
if [[ $# -ne 1 ]]; then
  echo "Invalid number of parameters. Please pass <ENV NAME>" > /dev/null
 
  exit 1
fi

env=$1
echo "Procesing enviorment is  :" $env > /dev/null

if [ "$env" != "home" ] ; then
  
  echo "Please provide required environment var. (home/taco/sit/uat/prod)" 
  
  exit 1
fi
 



##############################################################
# INVOKE PROJECT SPECIFIC  PARAMETERS
##############################################################

Dir_Name=${PWD}
export PROJECT_DIR=$(dirname ${Dir_Name})
export LOG_LOC=${PROJECT_DIR}/log
export SCRIPT_LOC=${PROJECT_DIR}/scripts
export HQL_LOC=${PROJECT_DIR}/hive
export HQL_TBL_LOC=${PROJECT_DIR}/hive/tables
export CONFIG_LOC=${PROJECT_DIR}/config
export SPARK_QUERY_LOC=${PROJECT_DIR}/spark
export LOGFILE=${LOG_LOC}/${Job_Name%%.*}_${env}_${JOB_START_TIME}.log
export CONFIG_FILE=${CONFIG_LOC}/${env}_config.txt
echo "\n START ${Job_Name}:  ${Start_Time}.\n" >> ${LOGFILE}

cfgfile=${CONFIG_LOC}/${env}_config.txt
echo "cfgfile :" $cfgfile >> ${LOGFILE}

export dq_db_nm=`grep dq_schema $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
#export hdfs_path=`grep hdfs_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_db_path=`grep dq_db_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_chk_master_path=`grep dq_chk_master_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_app_config_path=`grep dq_app_config_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`

echo "dq_database_nm :" $dq_db_nm >> ${LOGFILE}
#echo "hdfs_path :" $hdfs_path >> ${LOGFILE}
echo "dq_chk_master_path :" $dq_chk_master_path >> ${LOGFILE}
echo "dq_app_config_path :" $dq_app_config_path >> ${LOGFILE}

echo " ********** app_config Insert started *************************** ."

for app_config_fl in ${CONFIG_LOC}/dq_app_config*.txt
do
                echo $app_config_fl >> ${LOGFILE}
                
                
				echo "hadoop fs -rm "${dq_app_config_path}"/*" 
				      hadoop fs -rm "${dq_app_config_path}"/*
				echo "hadoop fs -put $app_config_fl "${dq_app_config_path}" " 
                      hadoop fs -put $app_config_fl "${dq_app_config_path}" >> ${LOGFILE}
	
	if [ $? -eq 0 ]; then
			
			echo "app_config  load completed for " $app_config_fl  >> ${LOGFILE}
	else
		    echo "app_config  load failed for " $app_config_fl  >> ${LOGFILE}
				
			exit 1 
	fi
	
echo " ********** app_config Insert Completed *************************** ."	
               
done

echo " ********** check_master Insert started *************************** ."

				echo "hadoop fs -rm "${dq_chk_master_path}"/*" 
				      hadoop fs -rm "${dq_chk_master_path}"/*
					  
for check_master_fl in ${CONFIG_LOC}/dq_check_master*.txt

do
                echo $check_master_fl >> ${LOGFILE}
                
                echo "hadoop fs -put $check_master_fl "${dq_chk_master_path}" " >> ${LOGFILE}
				      
                      hadoop fs -put $check_master_fl "${dq_chk_master_path}" >> ${LOGFILE}
	
	if [ $? -eq 0 ]; then
			
			echo "check_master  load completed for " $check_master_fl  >> ${LOGFILE}
	else
		    echo "check_master  load failed for " $check_master_fl  >> ${LOGFILE}
				
			exit 1 
	fi
	
echo " ********** check_master Insert Completed *************************** ."	
               
done

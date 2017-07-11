#!/bin/sh
################################################################################
# Program      : gen_dq_create_db_script.sh
# Date Created : 30/06/2017
# Description  :
# Parameters   :  <ENV NAME>
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 30/07/2017   Sanjeeb Panda                Creation
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
  
  echo "Please provide required environment var. (home/dev/sit/uat/prod)" 
  
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
export hdfs_path=`grep hdfs_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_db_path=`grep dq_db_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export hiveserver=`grep hiveserver2 $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}'`


echo "dq_database_nm :" $dq_db_nm >> ${LOGFILE}
echo "hdfs_path :" $hdfs_path >> ${LOGFILE}
echo "dq_database_path :" $dq_db_path>> ${LOGFILE}
echo "hiveserver :" $hiveserver>> ${LOGFILE}


case "$env" in
                sit)
                echo $env >> ${LOGFILE}
                echo "hiveserver :" $hiveserver >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "drop database if exists ${dq_db_nm} cascade;"
                echo beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
				     beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path ;"
				
                ;;  
                home)
                echo $env >> ${LOGFILE}
				echo "hiveserver :" $hiveserver >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "drop database if exists ${dq_db_nm} cascade;"
				echo beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path ;"
                ;; 				
                uat)
                echo $env >> ${LOGFILE}
                echo "hiveserver :" $hiveserver >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "drop database if exists ${dq_db_nm} cascade;"
                echo beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
				     beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path ;"
				;;
                prod)
                echo $env >> ${LOGFILE}
                echo "hiveserver :" $hiveserver >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "drop database if exists ${dq_db_nm} cascade;"
                echo beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path ;"
				
				;;
                dev)
                echo $env >> ${LOGFILE}
                echo "hiveserver :" $hiveserver >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "drop database if exists ${dq_db_nm} cascade;"
                echo beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
                     beeline -u $hiveserver --silent -e "create database $dq_db_nm location $dq_db_path ;"
				
				;;


esac

	if [ $? -eq 0 ]; then
			
			echo " ************************   Data Quality check execution  job $0 is completed in  $env for  $app_nm . *********************************">> ${LOGFILE}
	else
		    echo "************************    Data Quality check execution  job $0 is Failed    in  $env for   $app_nm .************************ ">> ${LOGFILE}
				exit 1 
	fi
			

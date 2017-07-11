#!/bin/sh
################################################################################
# Program      : gen_dq_ini_com.sh
# Date Created : 10/06/2017
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
if [[ $# -ne 3 ]]; then
  echo "Invalid number of parameters. Please pass <ENV NAME> <APPLICATION NM> <SOURCE SCHEMA NM> <SOURCE TBL NM>" > /dev/null
 
  exit 1
fi


# 1st Arguement: ENV NM.
# 2nd Arguement: APPLICATION NM.
# 3rd Arguement: SOURCE SCHEMA NM.
# 4th Arguement: SOURCE TBL NM.

env=$1
app_nm=$2
status=$3

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
export SPARK_SCRIPTS=${PROJECT_DIR}/spark
export LOGFILE=${LOG_LOC}/${Job_Name%%.*}_${env}_${JOB_START_TIME}.log
export CONFIG_FILE=${CONFIG_LOC}/${env}_config.txt
echo "\n START ${Job_Name}:  ${Start_Time}.\n" >> ${LOGFILE}

cfgfile=${CONFIG_LOC}/${env}_config.txt
echo "cfgfile :" $cfgfile >> ${LOGFILE}

export dq_db_nm=`grep dq_schema $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export hdfs_path=`grep hdfs_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_db_path=`grep dq_db_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_schema=`grep dq_schema $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`

echo "dq_database_nm :" $dq_db_nm >> ${LOGFILE}
echo "hdfs_path :" $hdfs_path >> ${LOGFILE}
echo "dq_database_path :" $dq_db_path>> ${LOGFILE}
echo "dq_schema :" $dq_schema>> ${LOGFILE}


case "$env" in
                sit)
                echo $env >> ${LOGFILE}
                echo "spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status"
                      spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status
                ;;  
                home)
                echo $env >> ${LOGFILE}
				echo "spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status"
					  spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status
                ;; 				
                uat)
                echo $env >> ${LOGFILE}
                echo $env >> ${LOGFILE}
                echo "spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status"
					  spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status
                ;;  
                prod)
                echo $env >> ${LOGFILE}
                echo "spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status"
					  spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status
                ;;  
                dev)
                echo $env >> ${LOGFILE}
                echo "spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status"
                      spark-submit ${SPARK_SCRIPTS}/gen_dq_prc_ini_com_spark.py  $dq_schema $app_nm $status
                ;;  
                


esac

	
	if [ $? -eq 0 ]; then
			
			echo " ************************  Data Quality check execution  job $0 is completed in  $env for  $app_nm $status *********************************">> ${LOGFILE}
	else
		    echo "************************    Data Quality check execution  job $0 is Failed   in     $env   for    $app_nm $status.************************ ">> ${LOGFILE}
				exit 1 
	fi
 

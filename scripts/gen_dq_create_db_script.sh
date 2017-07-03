#!/bin/sh
################################################################################
# Program      : create_db_script.sh
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

echo "dq_database_nm :" $dq_db_nm >> ${LOGFILE}
echo "hdfs_path :" $hdfs_path >> ${LOGFILE}
echo "dq_database_path :" $dq_db_path>> ${LOGFILE}


case "$env" in
                sit)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster02i1d.nam.nsroot.net:10000/default;principal=hive/bdgtmaster02i1d.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 --silent -e "drop database if exists ${dq_db_nm} cascade;"
                beeline -u $hiveserver2 --silent -e "create database ${dq_db_nm} location '/user/sp57529/data/${dq_db_nm}';"
                ;;  
                home)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://localhost:10000/default;"'
				echo "hiveserver2 :" $hiveserver2 >> ${LOGFILE}
                beeline -u $hiveserver2 --silent -e "drop database if exists ${dq_db_nm} cascade;"
				echo beeline -u $hiveserver2 --silent -e "create database $dq_db_nm location $dq_db_path;" >> ${LOGFILE}
                beeline -u $hiveserver2 --silent -e "create database $dq_db_nm location $dq_db_path ;"
                ;; 				
                uat)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster02i1u.nam.nsroot.net:10000/default;principal=hive/bdgtmaster02i1u.nam.nsroot.net@APACUXUAT.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;
                prod)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdsgmaster02i1p.nam.nsroot.net:10000/default;principal=hive/bdsgmaster02i1p.nam.nsroot.net@APACUXPRD.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;
                taco)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster12h1l.nam.nsroot.net:10000/default;principal=hive/bdgtmaster12h1l.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;


esac

	if [ $? -eq 0 ]; then
			
			echo " ************************  Data Base creation is completed in " $env   *********************************>> ${LOGFILE}
	else
		    echo "************************ Data Base creation is failed in " $env . "Please look into this." ************************ >> ${LOGFILE}
				exit 1 
	fi
				
 

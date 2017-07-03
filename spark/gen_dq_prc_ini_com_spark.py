from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from datetime import datetime
import datetime
import sys
import time

# TWO arguements are required to pass.
# 1st Arguement: DQ_SCHEMA_NM.
# 2nd Arguement: DQ_PRC_NM.
# 3rd Arguement: APP_NM.
# 4th Arguement: STATUS.



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="DQ APP")
    sqlContext=HiveContext(sc)
    # Command Line Arguement
    dq_schema=sys.argv[1]
    #prc_nm=sys.argv[2]
    dq_app_nm=sys.argv[2]
    status=sys.argv[3]
    job_status1= ['STARTED' if status=='INI' else 'COMPLETED' if status=='COM' else 'NONE']
    job_status=job_status1[0]
    if (job_status =='NONE'):
        print("Input Parameter has wrong values. It should be ini or com ", file=sys.stderr)
        exit(-1)
    else:
        
        dt1=datetime.datetime.now()
        dq_exec_start_tm=('%02d%02d%02d%02d%02d%02d'%(dt1.year,dt1.month,dt1.day,dt1.hour,dt1.minute,dt1.second))[:-2]
        #dq_batch_start_id=app_nm +'_'+('%02d%02d%02d%02d%02d%02d%d'%(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond))[:-4]
        dq_batch_start_dt=dt1.strftime('%Y%m%d')
        # DQ table details
        dq_app_config=dq_schema+'.'+'dq_app_config'
        dq_chk_master=dq_schema+'.'+'dq_chk_master'
        dq_log_detl_tbl_stg=dq_schema+'.'+'dq_log_detl_stg'
        dq_log_detl_tbl=dq_schema+'.'+'dq_log_detl'
        dq_reslt_detl_master_stg=dq_schema+'.'+'dq_result_detl_stg'
        dq_reslt_detl_master= dq_schema +'.'+'dq_result_detl'
        dq_reslt_summ_master=dq_schema+'.'+'dq_result_summary'
        print ("dq_schema : " ,dq_schema)
        print ("dq_app_config : " ,dq_app_config)
        print ("dq_reslt_summ_master : " ,dq_reslt_summ_master)
        print ("dq_chk_master : " ,dq_chk_master)
        print ("dq_reslt_detl_master_stg : " ,dq_reslt_detl_master_stg)
        print ("dq_reslt_detl_master : " ,dq_reslt_detl_master)
        print ("dq_log_detl_tbl_stg : " ,dq_log_detl_tbl_stg)
        print ("dq_log_detl_tbl : " ,dq_log_detl_tbl)
        
        
        dq_log_detl_col_list= ['dq_run_dt','dq_check_id','src_schema','src_col','dq_check_nm','dq_group_id','dq_app_id','rowkeynm',\
'dq_threshold_per','tot_rec','dq_run_status','dq_detl_hql','dq_run_time','src_tbl','dq_batch_id']

        print ("Seting Hive Parameter  " )

        sqlContext.setConf("hive.exec.dynamic.partition ","true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

        #print ("dq_batch_start_id:",dq_batch_start_id)
        print ("dq_batch_start_dt:",dq_batch_start_dt)
        # Selecting & Filtering the required configuration details from dq_app_config table.
        # If there is no matching record present as per the argument passed , processing will be stopped.
        dq_app_id = [x[0] for x in sqlContext.sql(" select dq_app_id from %s where  dq_app_name='%s' " %(dq_app_config,dq_app_nm)).collect()][0]
        #df_dq_app_config = sqlContext.sql("select dq_app_id from  %s where  dq_app_name='%s '" %(dq_app_config,dq_app_nm)).rdd.collect()
        #dfsql_sel_nxt_batch_id_ini=sqlContext.sql("select nvl(max(dq_batch_id),0)+1 batch_id from %s where  src_tbl='BATCH_EXE_STATS'" %(dq_log_detl_tbl_stg)).rdd
        dfsql_sel_nxt_batch_id_com=sqlContext.sql("select max(dq_batch_id) batch_id from %s where dq_app_id=%d and src_tbl='BATCH_EXE_STATS' " %(dq_log_detl_tbl_stg,dq_app_id))
        #dfsql_sel_nxt_batch_id_rdd_list=dfsql_sel_nxt_batch_id_ini.map(list)
        #dfsql_sel_nxt_batch_id_com_list=dfsql_sel_nxt_batch_id_com.map(list)
        if (status=='INI'):
            print ("all very very  good")
            batch_id=int(time.time())
            print ("batch_id_ini : " ,batch_id)
            dfsql_app_config=sqlContext.sql("select '%s' dq_run_dt, '' dq_check_id,'' src_schema,'' src_col,'DQ_BATCH_START' dq_check_nm,'' dq_group_id,'%s' dq_app_id,'' rowkeynm ,'' dq_threshold_per,'' tot_rec,'%s' dq_run_status,'' dq_detl_hql ,'BATCH_EXE_STATS' src_tbl from %s limit 1" %(dq_batch_start_dt,dq_app_id,job_status,dq_app_config))
            dfsql_app_config.show()
            dfsql_log_detl_stg=dfsql_app_config.withColumn("dq_run_time",lit(dq_exec_start_tm)).withColumn("dq_batch_id",lit(batch_id)).select(*dq_log_detl_col_list)
            dfsql_log_detl_stg.show()
            #dfsql_log_detl_stg.write.mode("append").partitionBy('src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
            dfsql_log_detl_stg.write.partitionBy('dq_batch_id','src_tbl').insertInto(dq_log_detl_tbl_stg)
        else:
            print ("all very very  good")
            batch_id = [x[0] for x in dfsql_sel_nxt_batch_id_com.map(list).collect()][0]
            print ("batch_id : " ,batch_id)
            print ("batch_id_com : " ,batch_id)
            dfsql_app_config=sqlContext.sql("select * from %s where dq_batch_id=%d and src_tbl='BATCH_EXE_STATS' " %(dq_log_detl_tbl_stg,batch_id))
            dt2=datetime.datetime.now()
            dq_run_end_time = ('%02d%02d%02d%02d%02d%02d'%(dt2.year,dt2.month,dt2.day,dt2.hour,dt2.minute,dt2.second))[:-2]
            dfsql_log_detl_stg=dfsql_app_config.withColumn("dq_run_time",lit(dq_run_end_time)).withColumn("dq_run_status",lit(job_status)).select(*dq_log_detl_col_list)
            dfsql_log_detl_stg.show()
            #dfsql_log_detl_stg.write.mode("append").partitionBy('src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
            dfsql_log_detl_stg.write.partitionBy('dq_batch_id','src_tbl').insertInto(dq_log_detl_tbl_stg)
                                                                                                        
            print (" ******* All entries in the dq_log_detl_tbl_stg is completed *******")
            print ("******** The Final Load process will start to load all the log information into dq_log_detl_tbl table .  ******")
            #time.sleep(120)
            print (" Spark Context will shut down")
            sc.stop()
            sc = SparkContext(appName="Aml Markets DQ")
            sqlContext=HiveContext(sc)
            print ("Seting Hive Parameter  " )
            sqlContext.setConf("hive.exec.dynamic.partition ","true")
            sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
            sqlContext.sql(''' insert overwrite table  %s partition(dq_batch_id,src_tbl) select *  from %s where dq_batch_id=%d ''' %(dq_log_detl_tbl,dq_log_detl_tbl_stg,batch_id))
        print (" Spark Context will shut down")
        print ("Intiation process is completed")
        print ("Spark Context will shut down")
    sc.stop()
    print ("Job is completed successfully")

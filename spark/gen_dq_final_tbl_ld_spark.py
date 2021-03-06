from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import rowNumber
from datetime import datetime
import datetime
import sys
# Three arguements are required to pass.
# 1st Arguement: Application NM.
# 2nd Arguement: Source Schema NM.
# 3rd Arguement: Source Tbl NM.
 
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="DQ Tbl Load")
    sqlContext=HiveContext(sc)
   
    app_nm=sys.argv[2]
    dq_schema=sys.argv[1]
    #src_schema=sys.argv[3]
   
    #dq_schema='amlmkt_dq'
    dq_app_config=dq_schema+'.'+'dq_app_config'
    dq_log_detl_tbl_stg=dq_schema+'.'+'dq_log_detl_stg'
    dq_log_detl_tbl=dq_schema+'.'+'dq_log_detl'
    dq_chk_master=dq_schema+'.'+'dq_chk_master'
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
   
    sqlContext.setConf("hive.exec.dynamic.partition ","true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    dq_app_id = [x[0] for x in sqlContext.sql(" select dq_app_id from %s where  dq_app_name='%s' " %(dq_app_config,app_nm)).collect()][0]
    batch_id_df=sqlContext.sql("select max(dq_batch_id) batch_id from %s where dq_app_id='%s' and src_tbl='BATCH_EXE_STATS' " %(dq_log_detl_tbl_stg,dq_app_id))
    batch_id = [x[0] for x in batch_id_df.map(list).collect()][0]
    print ("batch_id : " ,batch_id)
    # Inserting into dq_log_detl_tbl_stg  for check started..
    sqlContext.sql(''' insert overwrite table  %s partition(dq_batch_id,src_tbl) select *  from %s where dq_batch_id=%d ''' %(dq_log_detl_tbl,dq_log_detl_tbl_stg,batch_id))
    sqlContext.sql(''' insert overwrite table  %s partition(dq_batch_id,src_tbl) select *  from %s where dq_batch_id=%d ''' %(dq_reslt_detl_master,dq_reslt_detl_master_stg,batch_id))
    sqlContext.sql(''' insert overwrite table %s partition (dq_batch_id) \
select  dq_run_dt,dq_check_id,src_schema,src_tbl,src_col,dq_check_nm,dq_group_id,dq_app_id,rowkeynm,tot_rec,sum(nullchk) as null_chk_cnt, sum(dupchk) as dup_chk_cnt,sum(lenchk) as len_chk_cnt,\
sum(lovchk) as lov_chk_cnt,sum(datatypchk) as data_type_chk_cnt,sum(formatchk) as format_chk_cnt,sum(refchk) as ref_chk_cnt,sum(customchk) as custom_chk_cnt,\
from_unixtime(unix_timestamp(), 'yyyyMMddhhmmss') as load_time,dq_batch_id \
from %s where dq_batch_id=%d group by  dq_run_dt,dq_check_id,src_schema,src_tbl,src_col,dq_check_nm,dq_group_id,dq_app_id,rowkeynm,tot_rec,dq_batch_id ''' %(dq_reslt_summ_master,dq_reslt_detl_master,batch_id))
    
sc.stop()

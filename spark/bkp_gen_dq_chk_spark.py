from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from datetime import datetime
import datetime
import sys
import time
from dqgenquery import genQuery

sc = SparkContext(appName="Aml Markets DQ ")
sqlContext=HiveContext(sc)

app_nm='app_cust'
src_schema='cust_rf'
src_tbl='cust_prod_detl'
dq_schema='app_dq'
restart_flag='N'

dq_app_config=dq_schema+'.'+'dq_app_config'
dq_group_config=dq_schema+'.'+'dq_group_config'
dq_chk_master=dq_schema+'.'+'dq_chk_master'
dq_log_detl_tbl_stg=dq_schema+'.'+'dq_log_detl_stg'
dq_log_detl_tbl=dq_schema+'.'+'dq_log_detl'
dq_reslt_detl_master_stg=dq_schema+'.'+'dq_result_detl_stg'
dq_reslt_detl_master= dq_schema +'.'+'dq_result_detl'
dq_reslt_summ_master=dq_schema+'.'+'dq_reslt_summ_master'

dq_reslt_detl_col_list= ['dq_run_dt','dq_check_id','src_schema','src_col','dq_check_nm','dq_group_id','dq_app_id','rowkeynm','rowkeyval',\
'dq_threshold_per','tot_rec','nullchk','dupchk','lenchk','lovchk','datatypechk','formatchk','refchk','customchk','dq_run_time','dq_batch_id','src_tbl']
dq_log_detl_col_list= ['dq_run_dt','dq_check_id','src_schema','src_col','dq_check_nm','dq_group_id','dq_app_id','rowkeynm',\
'dq_threshold_per','tot_rec','dq_run_status','dq_detl_hql','dq_run_time','src_tbl','dq_batch_id']


print ("dq_schema : " ,dq_schema)
print ("dq_app_config : " ,dq_app_config)
print ("dq_reslt_summ_master : " ,dq_reslt_summ_master)
print ("dq_chk_master : " ,dq_chk_master)
print ("dq_reslt_detl_master_stg : " ,dq_reslt_detl_master_stg)
print ("dq_reslt_detl_master : " ,dq_reslt_detl_master)
print ("dq_log_detl_tbl_stg : " ,dq_log_detl_tbl_stg)
print ("dq_log_detl_tbl : " ,dq_log_detl_tbl)
print("src tbl",src_tbl)

print ("Seting Hive Parameter  " )
       
sqlContext.setConf("hive.exec.dynamic.partition ","true")
sqlContext.setConf("hive.exec.dynamic.partition.mode","true")
sqlContext.setConf("hive.execution.engine","spark")
sqlContext.setConf("hive.vectorized.execution.enabled","true")
sqlContext.setConf("hive.vectorized.execution.reduce.enabled","true")

dt=datetime.datetime.now()
dq_batch_start_id=app_nm +'_'+('%02d%02d%02d%02d%02d%02d%d'%(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond))[:-4]
dq_batch_start_dt=dt.strftime('%Y%m%d')
print ("dq_batch_start_id:",dq_batch_start_id)
print ("dq_batch_start_dt:",dq_batch_start_dt)

    
# Selecting & Filtering the required configuration details from dq_app_config table.
# If there is no matching record present as per the argument passed , processing will be stopped.

df_app_config = sqlContext.sql("select * from %s where dq_app_name = '%s' " %(dq_app_config,app_nm)).limit(1)
if df_app_config.count() == 0:
    print(" ************* DQ_APP_CONFIG has no matching record for %s . Please check the input Parameter or DQ_APP_CONFIG for further analysis  **************" %(src_tbl), file=sys.stderr)
    exit(-1)

print ("Application is registered in dq_app_config table")

dq_app_id = [x[0] for x in sqlContext.sql(" select dq_app_id from %s where  dq_app_name='%s' " %(dq_app_config,app_nm)).collect()][0]
#app_id=1
df_group_config = sqlContext.sql("select * from %s where dq_app_id = %d " %(dq_group_config,dq_app_id)).limit(1).cache()
df_group_config.show()
line =df_group_config.map(list).collect()
dq_grp_name= [str(x[0]).strip() for x in line][0]
dq_grp_id  = [str(x[1]).strip() for x in line][0]
dq_app_id  = [str(x[2]).strip() for x in line][0]
schema_name= [str(x[3]).strip() for x in line][0]
table_nm   = [str(x[4]).strip() for x in line][0]
filter     = [str(x[5]).strip() for x in line][0]
active_flag= [str(x[6]).strip() for x in line][0]


batch_id_df=sqlContext.sql("select max(dq_batch_id) batch_id from %s where dq_app_id='%s' and src_tbl='BATCH_EXE_STATS' " %(dq_log_detl_tbl_stg,dq_app_id))
batch_id = [x[0] for x in batch_id_df.map(list).collect()][0]
print ("batch_id : " ,batch_id)


cach_tbl_nm= table_nm
#query = "select * from %s.%s where %s " % (schema_name, table_nm, filter)
query = "select * from %s.%s  " % (schema_name, table_nm)
df_src_tbl_import = sqlContext.sql(query).cache()
tot_rec_cnt_src_tbl = df_src_tbl_import.count()

if tot_rec_cnt_src_tbl == 0:
    print(" ***************** %s.%s has 0 record for DQ processing. Please check source table for further processing. ***************" %(schema_nm,table_nm),file=sys.stderr)
    exit(-1)

df_src_tbl_import.registerTempTable(cach_tbl_nm)
sqlContext.cacheTable(cach_tbl_nm)
cach_tbl_cnt=sqlContext.sql('''select count(1) as cnt from %s ''' %(cach_tbl_nm))
cach_tbl_cnt_list=cach_tbl_cnt.map(list).collect()
cach_tbl_cnt1=[int(x[0]) for x in cach_tbl_cnt_list][0]
    # Cheking caching is done properly or not by comparing source tbl cnt and cahc tbl cnt..
print ("cach_tbl_cnt :" ,cach_tbl_cnt1)
print ("tot_rec_cnt_src_tbl :" , tot_rec_cnt_src_tbl)
if  ((cach_tbl_cnt1) !=(tot_rec_cnt_src_tbl)) :
    print(" ***************  %s couln't be cached properly. *****************" %(cach_tbl_nm), file=sys.stderr)
    exit(-1)



dfsql_chk_master=sqlContext.sql('''select * from %s where src_tbl ='%s'  '''%(dq_chk_master,table_nm) )
for line in dfsql_chk_master.map(list).collect():
    line1  = [str(x).strip() for x in line]
    output = genQuery(line1)
    query  = output[1]
    attributs = output[0].split("~")
    print (query)
    #df = sqlContext.sql(''' %s '''%(output))
    df_reslt_detl = sqlContext.sql(query)
    
    dq_run_dt   = dq_batch_start_dt
    dq_check_id = attributs[0]
    src_schema  = attributs[1]
    src_tbl     = attributs[2]
    src_col     = attributs[3]
    dq_check_nm = attributs[4]
    dq_group_id = attributs[5]
    dq_app_id   = attributs[6]
    rowkeynm    = attributs[7]
    dq_threshold_per = attributs[8]
    tot_rec     = tot_rec_cnt_src_tbl
    dt1=datetime.datetime.now()
    query1 = str(query)
    dq_run_start_time = ('%02d%02d%02d%02d%02d%02d'%(dt1.year,dt1.month,dt1.day,dt1.hour,dt1.minute,dt1.second))[:-2]
    dq_log_detl_stg_import =dfsql_chk_master.limit(1).select('dq_check_id')
    
    dq_log_detl_stg = dq_log_detl_stg_import.withColumn("dq_run_dt",lit(dq_run_dt)).withColumn("dq_check_id",lit(dq_check_id))\
                      .withColumn("src_schema",lit(src_schema)).withColumn("src_tbl",lit(src_tbl)).withColumn("src_col",lit(src_col))\
                      .withColumn("dq_check_nm",lit(dq_check_nm)).withColumn("dq_group_id",lit(dq_group_id)).withColumn("dq_app_id",lit(dq_app_id)) \
            .withColumn("rowkeynm",lit(rowkeynm)).withColumn("dq_threshold_per",lit(dq_threshold_per)).withColumn("tot_rec",lit(tot_rec)) \
            .withColumn("dq_run_status",lit('STARTED')).withColumn("dq_detl_hql",lit(query)).withColumn("dq_run_time",lit(dq_run_start_time)).withColumn("dq_batch_id",lit(batch_id))
    dq_log_detl_stg_start_ld =dq_log_detl_stg.select(*dq_log_detl_col_list)
    dq_log_detl_stg_start_ld.write.partitionBy('dq_batch_id','src_tbl').insertInto(dq_log_detl_tbl_stg)
    
    df_reslt_detl_ld = df_reslt_detl.withColumn("dq_run_dt",lit(dq_run_dt)).withColumn("dq_check_id",lit(dq_check_id))\
                      .withColumn("src_schema",lit(src_schema)).withColumn("src_tbl",lit(src_tbl)).withColumn("src_col",lit(src_col))\
                      .withColumn("dq_check_nm",lit(dq_check_nm)).withColumn("dq_group_id",lit(dq_group_id)).withColumn("dq_app_id",lit(dq_app_id)) \
            .withColumn("rowkeynm",lit(rowkeynm)).withColumn("dq_threshold_per",lit(dq_threshold_per)).withColumn("tot_rec",lit(tot_rec)) \
            .withColumn("dq_run_time",lit(dq_run_start_time)).withColumn("dq_batch_id",lit(batch_id))
    df_reslt_detl_final = df_reslt_detl_ld.select(*dq_reslt_detl_col_list)
    df_reslt_detl_final.write.partitionBy('dq_batch_id','src_tbl').insertInto(dq_reslt_detl_master_stg)
    
    dt2=datetime.datetime.now()
    dq_run_end_time = ('%02d%02d%02d%02d%02d%02d'%(dt2.year,dt2.month,dt2.day,dt2.hour,dt2.minute,dt2.second))[:-2]
    dq_log_detl_stg_end_ld = dq_log_detl_stg.withColumn("dq_run_time",lit(dq_run_end_time)).withColumn("dq_run_status",lit('COMPLETED')).select(*dq_log_detl_col_list)
    dq_log_detl_stg_end_ld.write.partitionBy('dq_batch_id','src_tbl').insertInto(dq_log_detl_tbl_stg)
sc.stop()

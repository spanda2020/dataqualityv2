CREATE  TABLE dq_log_detl_stg
(
dq_run_dt varchar(100),
dq_check_id int,
src_schema  varchar(100),
src_col  varchar(100),
dq_check_nm varchar(100),
dq_group_id int,
dq_app_id  varchar(100),
rowkeynm varchar(100),
dq_threshold_per float,
tot_rec int,
dq_run_status string,
dq_detl_hql string,
dq_run_time string)
PARTITIONED BY (
src_tbl varchar(100),
dq_batch_id int)
stored as parquetfile ;

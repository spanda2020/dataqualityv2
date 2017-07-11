use ${hiveconf:dq_schema};
create table dq_result_detl
(
dq_run_dt varchar(100),
dq_check_id int,
src_schema  varchar(100),
src_col  varchar(100),
dq_check_nm varchar(100),
dq_group_id int,
dq_app_id  varchar(100),
rowkeynm varchar(100),
rowkeyval varchar(100),
dq_threshold_per float,
tot_rec int,
nullChk varchar(1),
dupChk varchar(100),
lenChk varchar(100),
lovChk varchar(100),
dataTypChk varchar(100),
formatChk varchar(100),
refChk varchar(100),
customChk string,
dq_run_time varchar(100))
PARTITIONED BY (
dq_batch_id bigint,
src_tbl varchar(100))
stored as parquetfile ;

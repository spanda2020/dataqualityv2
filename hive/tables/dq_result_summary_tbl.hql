use ${hiveconf:dq_schema};
create table dq_result_summary
(
dq_run_dt varchar(100),
dq_check_id int,
src_schema  varchar(100),
src_tbl varchar(100),
src_col  varchar(100),
dq_check_nm varchar(100),
dq_group_id int,
dq_app_id  varchar(100),
rowkeynm varchar(100),
tot_rec varchar(1),
null_chk_cnt int,
dup_chk_cnt int,
len_chk_cnt int,
lov_chk_cnt int,
data_type_chk_cnt int,
format_chk_cnt int,
ref_chk_cnt int,
custom_chk_cnt int,
load_time varchar(20))
PARTITIONED BY (
dq_batch_id bigint)
stored as parquetfile ;

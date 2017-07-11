use ${hiveconf:dq_schema};
create table dq_result_summary
(
dq_check_id int,
src_tbl varchar(100),
dq_run_dt varchar(100),
dq_check_nm varchar(100),
dq_group_id int,
dq_app_id  varchar(100),
src_schema  varchar(100),
src_col  varchar(100),
rowkeynm varchar(100),
rowkeyval varchar(100),
dq_threshold_per float,
tot_rec_cnt varchar(1),
null_chk_cnt varchar(1),
dup_chk_cnt varchar(100),
len_chk_cnt varchar(100),
lov_chk_cnt varchar(100),
data_type_chk_cnt varchar(100),
format_chk_cnt varchar(100),
ref_chk_cnt varchar(100),
custom_chk_cnt string,
dq_run_time varchar(100))
PARTITIONED BY (
dq_batch_id bigint)
stored as parquetfile ;

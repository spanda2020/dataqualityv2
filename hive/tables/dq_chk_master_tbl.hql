use ${hiveconf:dq_schema};
create table dq_chk_master
(
dq_check_id int,
dq_check_nm varchar(100),
dq_group_id int,
dq_app_id  varchar(100),
src_schema  varchar(100),
src_tbl  varchar(100),
src_col  varchar(100),
rowkey varchar(100),
filter varchar(100),
dq_thrshold_per float,
null_chk varchar(1),
dup_chk varchar(100),
len_chk varchar(100),
lov_chk varchar(100),
data_type_chk varchar(100),
format_chk varchar(100),
ref_chk_schema varchar(100),
ref_chk_tbl varchar(100),
ref_chk_col varchar(100),
ref_chk_filter string,
custom_chk string,
error_rec_limit int,
active_flag varchar(1),
last_updt_dt varchar(50))
row format delimited fields terminated by '~' ;

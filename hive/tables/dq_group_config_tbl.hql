use ${hiveconf:dq_schema};
create table dq_group_config
(
dq_grp_name varchar(50),
dq_grp_id int,
dq_app_id int,
schema_name varchar(50),
table_nm varchar(50),
filter varchar(200),
active_flag varchar(1),
last_updt_dt varchar(50))
row format delimited fields terminated by '~' ;

use ${hiveconf:dq_schema};
create table DQ_APP_CONFIG
(
dq_app_name varchar(50),
dq_app_id int,
active_flag varchar(1),
last_updt_dt varchar(50))
row format delimited fields terminated by '~' ;

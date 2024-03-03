-- 插入数据前的hive配置设置
--分区
SET hive.exec.dynamic.partition=true; -- 开启动态分区
SET hive.exec.dynamic.partition.mode=nonstrict; --开启非严格模式
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000; --设置最大分区数
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

insert overwrite table edu_dwd.dwd_itcast_subject_dim partition (start_time)
select *
from edu_ods.ods_itcast_subject
where deleted = 0;
insert overwrite table edu_dwd.dwd_itcast_school_dim partition (start_time)
select *
from edu_ods.ods_itcast_school
where deleted = 0;
insert overwrite table edu_dwd.dwd_employee_dim partition (start_time)
select *
from edu_ods.ods_employee
where deleted = 0;
insert overwrite table edu_dwd.dwd_scrm_department_dim partition (start_time)
select *
from edu_ods.ods_scrm_department
where deleted = 0;
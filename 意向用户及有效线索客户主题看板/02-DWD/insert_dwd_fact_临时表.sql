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
-- 首次导入
insert overwrite table edu_dwd.dwd_customer_relationship_fact_tmp partition (start_date)
select *
from edu_dwd.dwd_customer_relationship_fact;

insert overwrite table edu_dwd.dwd_customer_clue_fact_tmp partition (start_date)
select *
from edu_dwd.dwd_customer_clue_fact;

insert overwrite table edu_dwd.dwd_customer_fact_tmp partition (start_date)
select *
from edu_dwd.dwd_customer_fact;

insert overwrite table edu_dwd.dwd_customer_appeal_fact partition (start_date)
select *
from edu_dwd.dwd_customer_appeal_fact;
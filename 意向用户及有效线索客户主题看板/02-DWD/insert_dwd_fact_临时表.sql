-- ��������ǰ��hive��������
--����
SET hive.exec.dynamic.partition=true; -- ������̬����
SET hive.exec.dynamic.partition.mode=nonstrict; --�������ϸ�ģʽ
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000; --������������
set hive.exec.max.created.files=150000;
--hiveѹ��
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--д��ʱѹ����Ч
set hive.exec.orc.compression.strategy=COMPRESSION;
-- �״ε���
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
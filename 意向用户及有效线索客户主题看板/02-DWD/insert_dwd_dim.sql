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
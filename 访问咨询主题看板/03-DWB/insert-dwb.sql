-- hive�ű�����
--����
SET hive.exec.dynamic.partition=true; -- ������̬����
SET hive.exec.dynamic.partition.mode=nonstrict; -- �������ϸ�ģʽ
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000; --������������
set hive.exec.max.created.files=150000;
--hiveѹ��
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--д��ʱѹ����Ч
set hive.exec.orc.compression.strategy=COMPRESSION;



insert overwrite table edu_dwb.visit_consult_dwb partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select
       -- ������ѯ�����ֶ�
       wce.id,
       wce.create_date_time,
       wce.session_id,
       wce.sid,
       wce.create_time,
       wce.seo_source,
       wce.seo_keywords,
       wce.ip,
       wce.area,
       wce.country,
       wce.province,
       wce.city,
       wce.origin_channel,
       wce.`user`,
       wce.manual_time,
       wce.begin_time,
       wce.end_time,
       wce.last_customer_msg_time_stamp,
       wce.last_agent_msg_time_stamp,
       wce.reply_msg_count,
       wce.msg_count,
       wce.browser_name,
       wce.os_info,
       wce.hourinfo,

       -- ������ѯ�������ֶ�
       wcte.referrer,
       wcte.from_url,
       wcte.landing_page_url,
       wcte.url_title,
       wcte.platform_description,
       wcte.other_params,
       wcte.history,

       -- ������ѯ�����ֶ� �����ֶ�
       wce.yearinfo,
       wce.quarterinfo,
       wce.monthinfo,
       wce.dayinfo
from edu_dwd.dwd_web_chat_ems_fact wce
         inner join edu_dwd.dwd_web_chat_text_ems_fact wcte
                    on wce.id = wcte.id;
-- hive脚本配置
--分区
SET hive.exec.dynamic.partition=true; -- 开启动态分区
SET hive.exec.dynamic.partition.mode=nonstrict; -- 开启非严格模式
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000; --设置最大分区数
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;



insert overwrite table edu_dwb.visit_consult_dwb partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select
       -- 访问咨询主表字段
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

       -- 访问咨询附属表字段
       wcte.referrer,
       wcte.from_url,
       wcte.landing_page_url,
       wcte.url_title,
       wcte.platform_description,
       wcte.other_params,
       wcte.history,

       -- 访问咨询主表字段 分区字段
       wce.yearinfo,
       wce.quarterinfo,
       wce.monthinfo,
       wce.dayinfo
from edu_dwd.dwd_web_chat_ems_fact wce
         inner join edu_dwd.dwd_web_chat_text_ems_fact wcte
                    on wce.id = wcte.id;
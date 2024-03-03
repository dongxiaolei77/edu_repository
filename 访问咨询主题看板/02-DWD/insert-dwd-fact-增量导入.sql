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

insert into table edu_dwd.dwd_web_chat_ems_fact partition (yearinfo, quarterinfo, monthinfo, dayinfo)
select id,
       create_date_time,
       session_id,
       sid,
       create_time,
       seo_source,
       seo_keywords,
       ip,
       area,
       country,
       province,
       city,
       origin_channel,
       `user`,
       manual_time,
       begin_time,
       end_time,
       last_customer_msg_time_stamp,
       last_agent_msg_time_stamp,
       reply_msg_count,
       msg_count,
       browser_name,
       os_info,
       substr(create_time, 12, 2) as hourinfo,
       substr(create_time, 0, 4)  as yearinfo,
       quarter(create_time)       as quarterinfo,
       substr(create_time, 6, 2)  as monthinfo,
       substr(create_time, 9, 2)  as dayinfo
from edu_ods.ods_web_chat_ems where dt = date_add(current_date(),-1);

insert into table edu_dwd.dwd_web_chat_text_ems_fact partition (dt)
select id,
       referrer,
       from_url,
       landing_page_url,
       url_title,
       platform_description,
       other_params,
       history,
       dt
from edu_ods.ods_web_chat_text_ems where dt=date_add(current_date(),-1);

select date_add(current_date(),-1);
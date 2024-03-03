#!/bin/bash

export HIVE_HOME=/usr/bin/hive

if [ $# == 1 ]

then
   dateStr=$1

else
   dateStr=`date -d '-1 day' +'%Y-%m-%d'`

fi


yearStr=`date -d ${dateStr} +'%Y'`
month_for_quarter=`date -d ${dateStr} +%-m`
quarterStr=$((($month_for_quarter-1)/3+1))
monthStr=`date -d ${dateStr} +'%m'`
dayStr=`date -d ${dateStr} +'%d'`

echo ${yearStr}
echo ${quarterStr}
echo ${monthStr}
echo ${dayStr}

hiveSql = "
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



insert into table edu_dwb.visit_consult_dwb partition (yearinfo, quarterinfo, monthinfo, dayinfo)
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
from (select *
      from edu_dwd.dwd_web_chat_ems_fact
      where yearinfo = year(date_add(current_date(),-1))
        and quarterinfo = quarter(date_add(current_date(),-1))
        and monthinfo = month(date_add(current_date(),-1))
        and dayinfo = day(date_add(current_date(),-1))) wce
         inner join
     (select *
      from edu_dwd.dwd_web_chat_text_ems_fact
      where dt = date_add(current_date(),-1)) wcte
     on wce.id = wcte.id;
"
${HIVE_HOME} -S -e '${hiveSql}'
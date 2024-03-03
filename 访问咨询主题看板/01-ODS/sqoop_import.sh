#!/bin/bash

export SQOOP_HOME=/usr/bin/sqoop

if [ $# == 1 ] 
then 
   dateStr=$1
   tableMonth=`date -d $1 +'%Y_%m'`
else 
   dateStr=`date -d '-1 day' +'%Y-%m-%d'`
   tableMonth=`date -d '-1 day' +'%Y_%m'`
fi

echo ${dateStr}

jdbcUrl='jdbc:mysql://106.75.33.59:3306/nev?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true'
username='bjclass58'
password='bjclass58_itcast'


${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as dt from web_chat_ems_${tableMonth} where 1=1 and (create_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table web_chat_ems \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1 


${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select t2.*, '${dateStr}' as dt from web_chat_ems_${tableMonth} t1 join web_chat_text_ems_${tableMonth} t2 on t1.id = t2.id  where 1=1 and (t1.create_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table web_chat_text_ems \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
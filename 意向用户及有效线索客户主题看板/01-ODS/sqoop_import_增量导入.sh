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
--query "select *, '${dateStr}' as dt from customer_relationship where 1=1 and (create_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') or (update_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59')  and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_customer_relationship \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as dt from customer where 1=1 and (create_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') or (update_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59')  and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_customer \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as dt from customer_clue where 1=1 and (create_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') or (update_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59')  and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_customer_clue \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as dt from customer_appeal where 1=1 and (create_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59') or (update_date_time between '${dateStr} 00:00:00' and '${dateStr} 23:59:59')  and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_customer_appeal \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
wait

# 由于维度表涉及的数据量都很小 且很少进行改动 所以不需要进行增量导入操作

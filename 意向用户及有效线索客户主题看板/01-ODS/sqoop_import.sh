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
--query "select *, '${dateStr}' as dt from customer_relationship where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
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
--query "select *, '${dateStr}' as dt from customer where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
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
--query "select *, '${dateStr}' as dt from customer_clue where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
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
--query "select *, '${dateStr}' as dt from customer_appeal where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_customer_appeal \
--hive-partition-key dt \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as start_time from itcast_subject where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_itcast_subject \
--hive-partition-key start_time \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as start_time from itcast_school  where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_itcast_school  \
--hive-partition-key start_time \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as start_time from employee  where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_employee  \
--hive-partition-key start_time \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as start_time from employee  where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_employee  \
--hive-partition-key start_time \
--hive-partition-value "${dateStr}" \
-m 1
wait

${SQOOP_HOME} import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select *, '${dateStr}' as start_time from scrm_department  where 1=1 and (create_date_time between '1970-01-01 00:00:00' and '${dateStr} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database edu_ods \
--hcatalog-table ods_scrm_department  \
--hive-partition-key start_time \
--hive-partition-value "${dateStr}" \
-m 1
wait
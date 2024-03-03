#! /bin/bash
year=`date -d '-1 day' +'%Y'`
#增量导出操作:
#先要将MySQL中当年的全部统计结果删除掉

mysql_sql = delete from edu_result.intention_valid_clue where yearinfo =${year}
mysql -h192.168.52.150 -uroot -p123456 -e "${mysql_sql}"

/usr/bin/sqoop export \
--connect "jdbc:mysql://192.168.52.150:3306/edu_result?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password '123456' \
--table intention_valid_clue \
--hcatalog-database edu_dm \
--hcatalog-table intention_valid_clue_dm \
--hcatalog-partition-keys yearinfo \
--hcatalog-partition-values ${year} \
-m 1
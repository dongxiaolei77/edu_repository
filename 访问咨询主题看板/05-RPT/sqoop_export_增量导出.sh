#! /bin/bash
#增量导出操作:
#先要将MySQL中当年的全部统计结果删除掉
#delete from edu_result.visit_dm where yearinfo ='2022'

/usr/bin/sqoop export \
--connect "jdbc:mysql://192.168.52.150:3306/edu_result?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password '123456' \
--table visit_consult \
--hcatalog-database edu_dm \
--hcatalog-table dm_visit_consult \
--hcatalog-partition-keys yearinfo \
--hcatalog-partition-values '2022' \
-m 1
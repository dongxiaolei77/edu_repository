#! /bin/bash

/usr/bin/sqoop export \
--connect "jdbc:mysql://192.168.52.150:3306/edu_rpt?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password '123456' \
--table intention_valid_clue \
--hcatalog-database edu_dm \
--hcatalog-table intention_valid_clue_dm \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by '\t' \
-m 1

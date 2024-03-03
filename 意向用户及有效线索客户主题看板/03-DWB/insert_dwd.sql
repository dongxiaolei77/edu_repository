-- hive设置
--分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;
--分桶
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.noconditionaltask=true;
--并行执行
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
--小文件合并
-- set mapred.max.split.size=2147483648;
-- set mapred.min.split.size.per.node=1000000000;
-- set mapred.min.split.size.per.rack=1000000000;
--矢量化查询
set hive.vectorized.execution.enabled=true;
--关联优化器
set hive.optimize.correlation=true;
--读取零拷贝
set hive.exec.orc.zerocopy=true;
--join数据倾斜
set hive.optimize.skewjoin=true;
-- set hive.skewjoin.key=100000;
set hive.optimize.skewjoin.compiletime=true;
set hive.optimize.union.remove=true;
-- group倾斜
set hive.groupby.skewindata=false;

with dcrf as (
    select *, '0' as valid_clue
    from edu_dwd.dwd_customer_relationship_fact dcrf
    where id not in
          (select customer_relationship_first_id
           from edu_dwd.dwd_customer_appeal_fact
           where appeal_status = 1
             and customer_relationship_first_id != 0
             and dwd_customer_appeal_fact.end_date = '9999-12-30')
    union all
    select *, '1' as valid_clue
    from edu_dwd.dwd_customer_relationship_fact dcrf
    where id in
          (select customer_relationship_first_id
           from edu_dwd.dwd_customer_appeal_fact
           where appeal_status = 1
             and customer_relationship_first_id != 0
             and dwd_customer_appeal_fact.end_date = '9999-12-30')
)
insert
overwrite
table
edu_dwb.intention_valid_clue_dwb
partition
(
yearinfo
,
monthinfo
)
select dcrf.customer_id,
       dcrf.valid_clue,
       dccf.new_old_state,
       dcrf.origin_type_state,
       dcf.area,
       disd.id    as itcast_subject_id,
       disd.name  as itcast_subject_name,
       disd2.id   as itcast_school_id,
       disd2.name as itcast_school_name,
       dcrf.origin_type,
       dsdd.id    as scrm_department_id,
       dsdd.name  as scrm_department_name,
       dcrf.hourinfo,
       dcrf.dayinfo,
       dcrf.yearinfo,
       dcrf.monthinfo

from dcrf
         inner join edu_dwd.dwd_customer_clue_fact dccf on dcrf.id = dccf.customer_relationship_id
         inner join edu_dwd.dwd_customer_fact dcf on dcrf.customer_id = dcf.id
         left join edu_dwd.dwd_itcast_subject_dim disd on dcrf.itcast_subject_id = disd.id
         left join edu_dwd.dwd_itcast_school_dim disd2 on dcrf.itcast_school_id = disd2.id
         left join edu_dwd.dwd_employee_dim ded on dcrf.creator = ded.id
         left join edu_dwd.dwd_scrm_department_dim dsdd on ded.tdepart_id = dsdd.id
where dcrf.end_date = '9999-12-30'
  and dccf.end_date = '9999-12-30'
  and dcf.end_date = '9999-12-30';




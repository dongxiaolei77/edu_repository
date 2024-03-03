#!/bin/bash

export HIVE_HOME=/usr/bin/hive

if [ $# == 1 ]
then
   dateStr=$1
else
   dateStr=`date -d'-1 day' +'%Y-%m-%d'`
fi


echo ${dateStr}

hiveSql="
-- 插入数据前的hive配置设置
--分区
SET hive.exec.dynamic.partition=true; -- 开启动态分区
SET hive.exec.dynamic.partition.mode=nonstrict; --开启非严格模式
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000; --设置最大分区数
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

-- 开始拉链表操作
-- customer_relationship 意向表 (事实表)
with temp as (
    select id,
           create_date_time,
           update_date_time,
           deleted,
           customer_id,
           first_id,
           belonger,
           belonger_name,
           initial_belonger,
           distribution_handler,
           business_scrm_department_id,
           last_visit_time,
           next_visit_time,
           origin_type,
           itcast_school_id,
           itcast_subject_id,
           intention_study_type,
           anticipat_signup_date,
           level,
           creator,
           current_creator,
           creator_name,
           origin_channel,
           comment,
           first_customer_clue_id,
           last_customer_clue_id,
           process_state,
           process_time,
           payment_state,
           payment_time,
           signup_state,
           signup_time,
           notice_state,
           notice_time,
           lock_state,
           lock_time,
           itcast_clazz_id,
           itcast_clazz_time,
           payment_url,
           payment_url_time,
           ems_student_id,
           delete_reason,
           deleter,
           deleter_name,
           delete_time,
           course_id,
           course_name,
           delete_comment,
           close_state,
           close_time,
           appeal_id,
           tenant,
           total_fee,
           belonged,
           belonged_time,
           belonger_time,
           transfer,
           transfer_time,
           follow_type,
           transfer_bxg_oa_account,
           transfer_bxg_belonger_name,
           if(origin_type in ('PRESIGNUP', 'NETSERVICE'), '线上', '线下') as origin_type_state,
           string(year(create_date_time))                             as yearinfo,
           string(month(create_date_time))                            as monthinfo,
           string(day(create_date_time))                              as dayinfo,
           string(hour(create_date_time))                             as hourinfo,
           '9999-12-30'                                               as end_date,
           dt                                                         as start_date
    from edu_ods.ods_customer_relationship
    where dt = date_add(current_date(), -1)
      and deleted = 0
    union all
    select dcrf.id,
           dcrf.create_date_time,
           dcrf.update_date_time,
           dcrf.deleted,
           dcrf.customer_id,
           dcrf.first_id,
           dcrf.belonger,
           dcrf.belonger_name,
           dcrf.initial_belonger,
           dcrf.distribution_handler,
           dcrf.business_scrm_department_id,
           dcrf.last_visit_time,
           dcrf.next_visit_time,
           dcrf.origin_type,
           dcrf.itcast_school_id,
           dcrf.itcast_subject_id,
           dcrf.intention_study_type,
           dcrf.anticipat_signup_date,
           dcrf.level,
           dcrf.creator,
           dcrf.current_creator,
           dcrf.creator_name,
           dcrf.origin_channel,
           dcrf.comment,
           dcrf.first_customer_clue_id,
           dcrf.last_customer_clue_id,
           dcrf.process_state,
           dcrf.process_time,
           dcrf.payment_state,
           dcrf.payment_time,
           dcrf.signup_state,
           dcrf.signup_time,
           dcrf.notice_state,
           dcrf.notice_time,
           dcrf.lock_state,
           dcrf.lock_time,
           dcrf.itcast_clazz_id,
           dcrf.itcast_clazz_time,
           dcrf.payment_url,
           dcrf.payment_url_time,
           dcrf.ems_student_id,
           dcrf.delete_reason,
           dcrf.deleter,
           dcrf.deleter_name,
           dcrf.delete_time,
           dcrf.course_id,
           dcrf.course_name,
           dcrf.delete_comment,
           dcrf.close_state,
           dcrf.close_time,
           dcrf.appeal_id,
           dcrf.tenant,
           dcrf.total_fee,
           dcrf.belonged,
           dcrf.belonged_time,
           dcrf.belonger_time,
           dcrf.transfer,
           dcrf.transfer_time,
           dcrf.follow_type,
           dcrf.transfer_bxg_oa_account,
           dcrf.transfer_bxg_belonger_name,
           dcrf.origin_type_state,
           dcrf.yearinfo,
           dcrf.monthinfo,
           dcrf.dayinfo,
           dcrf.hourinfo,
           if(ocr.id is null or dcrf.end_date != '9999-12-30', dcrf.end_date,
              date_add(current_date(), -1)) as end_date,
           dcrf.start_date
    from edu_dwd.dwd_customer_relationship_fact dcrf
             left join (select *
                        from edu_ods.ods_customer_relationship
                        where dt = current_date()) ocr
                       on ocr.id = dcrf.id
         -- 三十天内的数据才有可能变更
    where start_date >= date_add(current_date(), -30)
)
insert
overwrite
table
edu_dwd.dwd_customer_relationship_fact_tmp
partition
(
start_date
)
select *
from temp;

insert overwrite table edu_dwd.dwd_customer_relationship_fact partition (start_date)
select *
from edu_dwd.dwd_customer_relationship_fact_tmp;

-- customer(客户静态信息表)(事实表)
with temp as (
    select id,
           customer_relationship_id,
           create_date_time,
           update_date_time,
           deleted,
           name,
           idcard,
           birth_year,
           gender,
           phone,
           wechat,
           qq,
           email,
           area,
           leave_school_date,
           graduation_date,
           bxg_student_id,
           creator,
           origin_type,
           origin_channel,
           tenant,
           md_id,
           '9999-12-30' as end_date,
           dt           as start_time
    from edu_ods.ods_customer
    where dt = date_add(current_date(), -1)
      and deleted = 0
    union all
    select dcf.id,
           dcf.customer_relationship_id,
           dcf.create_date_time,
           dcf.update_date_time,
           dcf.deleted,
           dcf.name,
           dcf.idcard,
           dcf.birth_year,
           dcf.gender,
           dcf.phone,
           dcf.wechat,
           dcf.qq,
           dcf.email,
           dcf.area,
           dcf.leave_school_date,
           dcf.graduation_date,
           dcf.bxg_student_id,
           dcf.creator,
           dcf.origin_type,
           dcf.origin_channel,
           dcf.tenant,
           dcf.md_id,
           if(oc.id is null or dcf.end_date != '9999-12-30', dcf.end_date, date_add(current_date(), -1)) as end_date,
           dcf.start_date
    from edu_dwd.dwd_customer_fact dcf
             left join (select * from edu_ods.ods_customer where dt = date_add(current_date(), -1)) oc
                       on oc.id = dcf.id
    where start_date >= date_add(current_date(), -30)
)
insert
overwrite
table
edu_dwd.dwd_customer_fact_tmp
partition
(
start_date
)
select *
from temp;

insert overwrite table edu_dwd.dwd_customer_fact partition (start_date)
select *
from edu_dwd.dwd_customer_fact_tmp;

-- customer_clue 客户线索表(事实表)
with temp as (
    select id,
           create_date_time,
           update_date_time,
           deleted,
           customer_id,
           customer_relationship_id,
           session_id,
           sid,
           `status`,
           `user`,
           create_time,
           platform,
           s_name,
           seo_source,
           seo_keywords,
           ip,
           referrer,
           from_url,
           landing_page_url,
           url_title,
           to_peer,
           manual_time,
           begin_time,
           reply_msg_count,
           total_msg_count,
           msg_count,
           `comment`,
           finish_reason,
           finish_user,
           end_time,
           platform_description,
           browser_name,
           os_info,
           area,
           country,
           province,
           city,
           creator,
           name,
           idcard,
           phone,
           itcast_school_id,
           itcast_school,
           itcast_subject_id,
           itcast_subject,
           wechat,
           qq,
           email,
           gender,
           `level`,
           origin_type,
           information_way,
           working_years,
           technical_directions,
           customer_state,
           valid,
           anticipat_signup_date,
           clue_state,
           scrm_department_id,
           superior_url,
           superior_source,
           landing_url,
           landing_source,
           info_url,
           info_source,
           origin_channel,
           course_id,
           course_name,
           zhuge_session_id,
           is_repeat,
           tenant,
           activity_id,
           activity_name,
           follow_type,
           shunt_mode_id,
           shunt_employee_group_id,
           case
               when clue_state = 'VALID_NEW_CLUES' then '新客户'
               when clue_state = 'VALID_PUBLIC_NEW_CLUE' then '老客户'
               else null end as new_old_state,
           '9999-12-30'      as end_date,
           dt                as start_date
    from edu_ods.ods_customer_clue
    where dt = date_add(current_date(), -1)
      and deleted = 0
    union all
    select dccf.id,
           dccf.create_date_time,
           dccf.update_date_time,
           dccf.deleted,
           dccf.customer_id,
           dccf.customer_relationship_id,
           dccf.session_id,
           dccf.sid,
           dccf.status,
           dccf.`user`,
           dccf.create_time,
           dccf.platform,
           dccf.s_name,
           dccf.seo_source,
           dccf.seo_keywords,
           dccf.ip,
           dccf.referrer,
           dccf.from_url,
           dccf.landing_page_url,
           dccf.url_title,
           dccf.to_peer,
           dccf.manual_time,
           dccf.begin_time,
           dccf.reply_msg_count,
           dccf.total_msg_count,
           dccf.msg_count,
           dccf.`comment`,
           dccf.finish_reason,
           dccf.finish_user,
           dccf.end_time,
           dccf.platform_description,
           dccf.browser_name,
           dccf.os_info,
           dccf.area,
           dccf.country,
           dccf.province,
           dccf.city,
           dccf.creator,
           dccf.name,
           dccf.idcard,
           dccf.phone,
           dccf.itcast_school_id,
           dccf.itcast_school,
           dccf.itcast_subject_id,
           dccf.itcast_subject,
           dccf.wechat,
           dccf.qq,
           dccf.email,
           dccf.gender,
           dccf.level,
           dccf.origin_type,
           dccf.information_way,
           dccf.working_years,
           dccf.technical_directions,
           dccf.customer_state,
           dccf.valid,
           dccf.anticipat_signup_date,
           dccf.clue_state,
           dccf.scrm_department_id,
           dccf.superior_url,
           dccf.superior_source,
           dccf.landing_url,
           dccf.landing_source,
           dccf.info_url,
           dccf.info_source,
           dccf.origin_channel,
           dccf.course_id,
           dccf.course_name,
           dccf.zhuge_session_id,
           dccf.is_repeat,
           dccf.tenant,
           dccf.activity_id,
           dccf.activity_name,
           dccf.follow_type,
           dccf.shunt_mode_id,
           dccf.shunt_employee_group_id,
           dccf.new_old_state,
           if(occ.id is null or dccf.end_date != '9999-12-30', dccf.end_date, date_add(current_date(), -1)) as end_date,
           dccf.start_date
    from edu_dwd.dwd_customer_clue_fact dccf
             left join (select * from edu_ods.ods_customer_clue where dt = date_add(current_date(), -1)) occ
                       on dccf.id = occ.id
    where start_date >= date_add(current_date(), -30)
)
insert
overwrite
table
edu_dwd.dwd_customer_clue_fact_tmp
partition
(
start_date
)
select *
from temp;

insert overwrite table edu_dwd.dwd_customer_clue_fact partition (start_date)
select *
from edu_dwd.dwd_customer_clue_fact_tmp;


-- customer_appeal申诉表 (事实表)
with temp as (
    select id,
           customer_relationship_first_id,
           employee_id,
           employee_name,
           employee_department_id,
           employee_tdepart_id,
           appeal_status,
           audit_id,
           audit_name,
           audit_department_id,
           audit_department_name,
           audit_date_time,
           create_date_time,
           update_date_time,
           deleted,
           tenant,
           '9999-12-30' as end_date,
           dt           as start_date
    from edu_ods.ods_customer_appeal
    where dt = date_add(current_date(), -1) and deleted = 'false'
    union all
    select dcaf.id,
           dcaf.customer_relationship_first_id,
           dcaf.employee_id,
           dcaf.employee_name,
           dcaf.employee_department_id,
           dcaf.employee_tdepart_id,
           dcaf.appeal_status,
           dcaf.audit_id,
           dcaf.audit_name,
           dcaf.audit_department_id,
           dcaf.audit_department_name,
           dcaf.audit_date_time,
           dcaf.create_date_time,
           dcaf.update_date_time,
           dcaf.deleted,
           dcaf.tenant,
           if(oca.id is null or dcaf.end_date != '9999-12-30', dcaf.end_date, date_add(current_date, -1)) as end_date,
           dcaf.start_date
    from edu_dwd.dwd_customer_appeal_fact dcaf
             left join (select * from edu_ods.ods_customer_appeal where dt = date_add(current_date, -1)) oca
                       on dcaf.id = oca.id
    where start_date >= date_add(current_date, -30)
)
insert
overwrite
table
edu_dwd.dwd_customer_appeal_fact_tmp
partition
(
start_date
)
select *
from temp;

insert overwrite table edu_dwd.dwd_customer_appeal_fact partition (start_date)
select *
from edu_dwd.dwd_customer_appeal_fact_tmp;
"


${HIVE_HOME} -S -e "${hiveSql}"
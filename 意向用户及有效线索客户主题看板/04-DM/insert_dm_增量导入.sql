# 以下代码在hive中运行
--
-- alter table edu_dm.intention_valid_clue_dm drop partition (year = '当前时间')
# 以下代码在presto中运行

-- 以下代码在presto中运行

insert into edu_dm.intention_valid_clue_dm
select
    -- 指标字段计算
    count (distinct customer_id) as count_intention,
    if(
    grouping (area)=0
    or grouping (itcast_subject_id,itcast_subject_name)=0
    or grouping (itcast_school_id,itcast_school_name)=0
    or grouping (origin_type)=0
    or grouping (scrm_department_id,scrm_department_name) =0,
    null,
    count (if(valid_clue='0',customer_id,null))
    ) as count_valid_clue,
    -- 经验字段
    case when grouping (hourinfo)=0 then '1'
         when grouping (dayinfo)=0 then '2'
         when grouping (monthinfo)=0 then '3'
         when grouping (yearinfo)=0 then '4'
         else null end  as time_type,
    case when grouping (area)=0 then '1'
         when grouping (itcast_subject_id,itcast_subject_name)=0 then '2'
         when grouping (itcast_school_id,itcast_school_name)=0 then '3'
         when grouping (origin_type)=0 then '4'
         when grouping (scrm_department_id,scrm_department_name)=0 then '5'
         else null end as group_type,
    -- 维度字段
    case when grouping (new_old_state)=0 then new_old_state else '-1' end as new_old_state,
    case when grouping (origin_type_state)=0 then origin_type_state else '-1' end as new_old_state,
    case when grouping (area)=0 then area else '-1' end as area,
    case when grouping (itcast_subject_id,itcast_subject_name) =0 then itcast_subject_id else '-1' end itcast_subject_id,
    case when grouping (itcast_subject_id,itcast_subject_name) =0 then itcast_subject_name else '-1' end itcast_subject_name,
    case when grouping (itcast_school_id,itcast_school_name)=0 then itcast_school_id else '-1' end itcast_school_id,
    case when grouping (itcast_school_id,itcast_school_name)=0 then itcast_school_name else '-1' end itcast_school_name,
    case when grouping (origin_type)=0 then origin_type else '-1' end as origin_type,
    case when grouping (scrm_department_id,scrm_department_name) =0 then scrm_department_id else '-1' end as scrm_department_id,
    case when grouping (scrm_department_id,scrm_department_name) =0 then scrm_department_name else '-1' end as scrm_department_name,
    case when grouping (hourinfo)=0 then hourinfo else '-1' end as hourinfo,
    case when grouping (dayinfo)=0 then dayinfo else '-1' end as dayinfo,
    case when grouping (yearinfo)=0 then yearinfo else '-1' end as yearinfo,
    case when grouping (monthinfo)=0 then monthinfo else '-1' end as monthinfo
 from edu_dwb.intention_valid_clue_dwb where yearinfo = year(date_add(current_date,-1))
 group by
 grouping sets(
    (yearinfo),
    (yearinfo,monthinfo),
    (yearinfo,monthinfo,dayinfo),
    (yearinfo,monthinfo,dayinfo,hourinfo),
    (yearinfo,new_old_state),
    (yearinfo,monthinfo,new_old_state),
    (yearinfo,monthinfo,dayinfo,new_old_state),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state),
    (yearinfo,origin_type_state),
    (yearinfo,monthinfo,origin_type_state),
    (yearinfo,monthinfo,dayinfo,origin_type_state),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state),
    (yearinfo,new_old_state,origin_type_state),
    (yearinfo,monthinfo,new_old_state,origin_type_state),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state),
    (yearinfo,area),
    (yearinfo,monthinfo,area),
    (yearinfo,monthinfo,dayinfo,area),
    (yearinfo,monthinfo,dayinfo,hourinfo,area),
    (yearinfo,new_old_state,area),
    (yearinfo,monthinfo,new_old_state,area),
    (yearinfo,monthinfo,dayinfo,new_old_state,area),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,area),
    (yearinfo,origin_type_state,area),
    (yearinfo,monthinfo,origin_type_state,area),
    (yearinfo,monthinfo,dayinfo,origin_type_state,area),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state,area),
    (yearinfo,new_old_state,origin_type_state,area),
    (yearinfo,monthinfo,new_old_state,origin_type_state,area),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state,area),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state,area),
    (yearinfo,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,itcast_subject_id,itcast_subject_name),
    (yearinfo,new_old_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,new_old_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,new_old_state,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,new_old_state,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state,itcast_subject_id,itcast_subject_name),
    (yearinfo,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,itcast_school_id,itcast_school_name),
    (yearinfo,new_old_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,new_old_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,itcast_school_id,itcast_school_name),
    (yearinfo,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,new_old_state,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,new_old_state,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state,itcast_school_id,itcast_school_name),
    (yearinfo,origin_type),
    (yearinfo,monthinfo,origin_type),
    (yearinfo,monthinfo,dayinfo,origin_type),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type),
    (yearinfo,new_old_state,origin_type),
    (yearinfo,monthinfo,new_old_state,origin_type),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type),
    (yearinfo,origin_type_state,origin_type),
    (yearinfo,monthinfo,origin_type_state,origin_type),
    (yearinfo,monthinfo,dayinfo,origin_type_state,origin_type),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state,origin_type),
    (yearinfo,new_old_state,origin_type_state,origin_type),
    (yearinfo,monthinfo,new_old_state,origin_type_state,origin_type),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state,origin_type),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state,origin_type),
    (yearinfo,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,scrm_department_id,scrm_department_name),
    (yearinfo,new_old_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,new_old_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,scrm_department_id,scrm_department_name),
    (yearinfo,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,new_old_state,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,new_old_state,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,new_old_state,origin_type_state,scrm_department_id,scrm_department_name),
    (yearinfo,monthinfo,dayinfo,hourinfo,new_old_state,origin_type_state,scrm_department_id,scrm_department_name)
    )
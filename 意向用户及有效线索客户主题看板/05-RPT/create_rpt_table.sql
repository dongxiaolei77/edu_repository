# 这些内容在mysql中执行
drop table if exists edu_rpt.intention_valid_clue;
create table if not exists edu_rpt.intention_valid_clue
(
    -- 指标字段
    count_intention      bigint comment '总意向量',
    count_valid_clue     bigint comment '有效线索量',
    -- 经验字段
    time_type            varchar(500) comment '判断时间维度 1为小时 2为天 3为月 4为年',
    group_type           varchar(500) comment '判断其他维度 1为区域 2为学科 3为校区 4为来源渠道 5为咨询中心',
    -- 维度字段
    new_old_state        varchar(500) comment '判断新老客户 涉及新老维度就有值 不涉及 新老维度就为 -1',
    origin_type_state    varchar(500) comment '判断线上线下 涉及线上线下就有之 不涉及 线上线下维度就为 -1',
    area                 varchar(500) comment '区域',
    itcast_subject_id    varchar(500) comment '学科id',
    itcast_subject_name  varchar(500) comment '学科名',
    itcast_school_id     varchar(500) comment '校区id',
    itcast_school_name   varchar(500) comment '校区名',
    origin_type          varchar(500) comment '来源渠道',
    scrm_department_id   varchar(500) comment '咨询中心id',
    scrm_department_name varchar(500) comment '咨询中心名称',
    hourinfo             varchar(500) comment '小时',
    dayinfo              varchar(500) comment '天',
    yearinfo              varchar(500) comment '年份',
    monthinfo              varchar(500) comment '月份'
) comment '意向客户及有效线索主题'
;

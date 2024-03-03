drop table if exists edu_dm.intention_valid_clue_dm;
create table if not exists edu_dm.intention_valid_clue_dm
(
    -- 指标字段
    count_intention      bigint comment '总意向量',
    count_valid_clue     bigint comment '有效线索量',
    -- 经验字段
    time_type            string comment '判断时间维度 1为小时 2为天 3为月 4为年',
    group_type           string comment '判断其他维度 1为区域 2为学科 3为校区 4为来源渠道 5为咨询中心',
    -- 维度字段
    new_old_state        string comment '判断新老客户 涉及新老维度就有值 不涉及 新老维度就为 -1',
    origin_type_state    string comment '判断线上线下 涉及线上线下就有之 不涉及 线上线下维度就为 -1',
    area                 string comment '区域',
    itcast_subject_id    string comment '学科id',
    itcast_subject_name  string comment '学科名',
    itcast_school_id     string comment '校区id',
    itcast_school_name   string comment '校区名',
    origin_type          string comment '来源渠道',
    scrm_department_id   string comment '咨询中心id',
    scrm_department_name string comment '咨询中心名称',
    hourinfo             string comment '小时',
    dayinfo              string comment '天'
) comment '意向客户及有效线索主题'
    partitioned by (yearinfo string, monthinfo string)
    row format delimited fields terminated by '\t';
drop table if exists edu_dwb.intention_valid_clue_dwb;
create table if not exists edu_dwb.intention_valid_clue_dwb
(
    -- 指标字段
    customer_id          string comment '客户id',
    valid_clue           string comment '是否为有效线索 有效为0 无效为1',
    -- 维度字段
    new_old_state        string comment '判断新老客户',
    origin_type_state    string comment '判断线上线下',
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
) comment '意向客户及有效线索主题宽表'
    partitioned by (yearinfo string, monthinfo string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );
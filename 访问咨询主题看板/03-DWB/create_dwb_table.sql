drop table if exists edu_dwb.visit_consult_dwb;
create table if not exists edu_dwb.visit_consult_dwb
(
    -- 访问咨询主表字段 DWD层
    id                           int comment '主键',
    create_date_time             string comment '数据创建时间',
    session_id                   string comment '七陌SESsionId',
    sid                          string comment '访客id',
    create_time                  string comment '会话创建时间',
    seo_source                   string comment '搜索来源',
    seo_keywords                 string comment '关键字',
    ip                           string comment 'IP地址',
    AREA                         string comment '地域',
    country                      string comment '所在国家',
    province                     string comment '省',
    city                         string comment '城市',
    origin_channel               string comment '投放渠道',
    `user`                       string comment '所属坐席',
    manual_time                  string comment '人工开始时间',
    begin_time                   string comment '坐席领取时间',
    end_time                     string comment '会话结束时间',
    last_customer_msg_time_stamp string comment '客户最后一条消息的时间',
    last_agent_msg_time_stamp    string comment '坐席最后一下回复的时间',
    reply_msg_count              int comment '客服回复消息数',
    msg_count                    int comment '客户发送消息数',
    browser_name                 string comment '浏览器名称',
    os_info                      string comment '系统名称',
    hourinfo                     string comment '数据创建时间-小时',

    -- 访问咨询记录附属表 副表字段 DWD层
    referrer                     string comment '上级来源页面',
    from_url                     string comment '会话来源页面',
    landing_page_url             string comment '访客着陆页面',
    url_title                    string comment '咨询页面title',
    platform_description         string comment '客户平台信息',
    other_params                 string comment '扩展字段中数据',
    history                      string comment '历史访问记录'


) partitioned by (yearinfo string, quarterinfo string, monthinfo string, dayinfo string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );
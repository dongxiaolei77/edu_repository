drop table if exists edu_dm.dm_visit_consult;
create table if not exists edu_dm.dm_visit_consult
(
    -- 指标字段
    sid_visit      bigint comment '访问量',
    sid_consult    bigint comment '咨询量',
    -- 维度字段
    hourinfo       string comment '小时',
    area           string comment '地区维度',
    origin_channel string comment '来源渠道维度',
    seo_source     string comment '搜索来源维度',
    from_url       string comment '受访url维度',
    --分类标签
    time_type      string comment '时间标记: 1 小时  2 天 3月 4 季度 5 年',
    group_type     string comment '产品属性维度标记: 1 地区  2 来源渠道 3 搜索来源 4 受访url 5总访问量'

) partitioned by (yearinfo string, quarterinfo string, monthinfo string, dayinfo string)
    row format delimited fields terminated by '\t';
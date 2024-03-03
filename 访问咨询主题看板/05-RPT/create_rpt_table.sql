
drop table if exists edu_rpt.visit_consult;

create table if not exists edu_rpt.visit_consult
(
    sid_visit      bigint comment '访问量',
    sid_consult    bigint comment '咨询量量',
    area           text comment '地区维度',
    origin_channel text comment '来源渠道维度',
    seo_source     text comment '搜索来源维度',
    from_url       text comment '受访url',
    time_type      text comment '时间标记: 1 小时  2 天 3月 4 季度 5 年',
    group_type     text comment '产品属性维度标记: 1 地区  2 来源渠道 3 搜索来源 4 受访url 5总访问量',
    yearinfo       text comment '年份',
    quarterinfo    text comment '季度',
    monthinfo      text comment '月份',
    dayinfo        text comment '天',
    hourinfo       text comment '小时'
);
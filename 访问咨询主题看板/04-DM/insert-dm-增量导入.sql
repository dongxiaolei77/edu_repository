-- 删除当前年份的分区表
-- alter table edu_dm.dm_visit_consult partition (yearinfo=当前年份);

insert into edu_dm.dm_visit_consult
with temp as (select
                  -- 指标字段
                  sid,
                  msg_count,
                  -- 维度字段
                  area,           -- 地区维度
                  origin_channel, -- 来源渠道维度
                  seo_source,     -- 搜索来源维度
                  from_url,       -- 受访页面维度
                  yearinfo,       -- 年
                  quarterinfo,    -- 季度
                  monthinfo,      -- 月
                  dayinfo,        --天
                  hourinfo        -- 小时
              from edu_dwb.visit_consult_dwb
              where yearinfo = year(current_date))
select count(distinct sid)                                                      as sid_visit,
       count(distinct if(msg_count >= 1, sid, null))                            as sid_consult,
       case when grouping(hourinfo) = 0 then hourinfo else null end             as hourinfo,
       case when grouping(area) = 0 then area else null end                     as area,
       case when grouping(origin_channel) = 0 then origin_channel else null end as origin_channel,
       case when grouping(seo_source) = 0 then seo_source else null end         as seo_source,
       case when grouping(from_url) = 0 then from_url else null end             as from_url,
       case when grouping(dayinfo) = 0 then dayinfo else null end               as dayinfo,
       case when grouping(monthinfo) = 0 then monthinfo else null end           as monthinfo,
       case when grouping(quarterinfo) = 0 then quarterinfo else null end       as quarterinfo,
       case when grouping(yearinfo) = 0 then yearinfo else null end             as yearinfo,
       case
           when grouping(yearinfo, quarterinfo, monthinfo, dayinfo, hourinfo) = 0 then '1'
           when grouping(yearinfo, quarterinfo, monthinfo, dayinfo) = 0 then '2'
           when grouping(yearinfo, quarterinfo, monthinfo) = 0 then '3'
           when grouping(yearinfo, quarterinfo) = 0 then '4'
           when grouping(yearinfo) = 0 then '5'
           else 'all' end                                                       as time_type,
       case
           when grouping(area) = 0 then '1'
           when grouping(origin_channel) = 0 then '2'
           when grouping(seo_source) = 0 then '3'
           when grouping(from_url) = 0 then '4'
           else 'all' end                                                       as group_type
from temp
group by grouping sets
        ((yearinfo),
        (yearinfo,quarterinfo),
        (yearinfo,quarterinfo,monthinfo),
        (yearinfo,quarterinfo,monthinfo,dayinfo),
        (yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo),
        (yearinfo,area),
        (yearinfo,quarterinfo,area),
        (yearinfo,quarterinfo,monthinfo,area),
        (yearinfo,quarterinfo,monthinfo,dayinfo,area),
        (yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,area),
        (yearinfo,origin_channel),
        (yearinfo,quarterinfo,origin_channel),
        (yearinfo,quarterinfo,monthinfo,origin_channel),
        (yearinfo,quarterinfo,monthinfo,dayinfo,origin_channel),
        (yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,origin_channel),
        (yearinfo,seo_source),
        (yearinfo,quarterinfo,seo_source),
        (yearinfo,quarterinfo,monthinfo,seo_source),
        (yearinfo,quarterinfo,monthinfo,dayinfo,seo_source),
        (yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,seo_source),
        (yearinfo,from_url),
        (yearinfo,quarterinfo,from_url),
        (yearinfo,quarterinfo,monthinfo,from_url),
        (yearinfo,quarterinfo,monthinfo,dayinfo,from_url),
        (yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,from_url)
        )

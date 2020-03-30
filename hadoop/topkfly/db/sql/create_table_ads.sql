-- 1. 当月、当日、当周活跃设备数
drop table if exists gmall.ads_uv_count;
create external table gmall.ads_uv_count(
`dt` string COMMENT '统计日期',
`day_count` bigint COMMENT '当日用户数量',
`wk_count`  bigint COMMENT '当周用户数量',
`mn_count`  bigint COMMENT '当月用户数量',
`is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
`is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';

insert into table gmall.ads_uv_count
select
    '2019-02-10' dt,
    daycount.ct,
    wkcount.ct,
    mncount.ct,
    if(date_add(next_day('2019-02-10','MO'),-1)='2019-02-10','Y','N') ,
    if(last_day('2019-02-10')='2019-02-10','Y','N')
from
    (
        select
            '2019-02-10' dt,
            count(*) ct
        from gmall.dws_uv_detail_day
        where dt='2019-02-10'
    )daycount join
    (
        select
            '2019-02-10' dt,
            count (*) ct
        from gmall.dws_uv_detail_wk
        where wk_dt=concat(date_add(next_day('2019-02-10','MO'),-7),'_' ,date_add(next_day('2019-02-10','MO'),-1) )
    ) wkcount on daycount.dt=wkcount.dt
              join
    (
        select
            '2019-02-10' dt,
            count (*) ct
        from gmall.dws_uv_detail_mn
        where mn=date_format('2019-02-10','yyyy-MM')
    )mncount on daycount.dt=mncount.dt;


-- 用户留存数
drop table if exists gmall.ads_user_retention_day_count;
create external table gmall.ads_user_retention_day_count
(
`create_date`     string  comment '设备新增日期',
`retention_day`   int comment '截止当前日期留存天数',
`retention_count` bigint comment  '留存数量'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_count/';

insert into table gmall.ads_user_retention_day_count
select
    create_date,
    retention_day,
    count(*) retention_count
from gmall.dws_user_retention_day
where dt='2019-02-11'
group by create_date,retention_day;

-- 用户留存率
drop table if exists gmall.ads_user_retention_day_rate;

create external table gmall.ads_user_retention_day_rate
(
`stat_date`        string comment '统计日期',
`create_date`      string  comment '设备新增日期',
`retention_day`    int comment '截止当前日期留存天数',
`retention_count`  bigint comment  '留存数量',
`new_mid_count`    bigint comment '当日设备新增数量',
`retention_ratio`  decimal(10,2) comment '留存率'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

insert into table gmall.ads_user_retention_day_rate
select
    '2019-02-11',
    ur.create_date,
    ur.retention_day,
    ur.retention_count,
    nc.new_mid_count,
    ur.retention_count/nc.new_mid_count*100
from gmall.ads_user_retention_day_count ur join gmall.ads_new_mid_count nc
                                                on nc.create_date=ur.create_date;
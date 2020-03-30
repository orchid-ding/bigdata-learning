-- 1. 日活跃设备分析

drop table if exists gmall.dws_uv_detail_day;
create external table gmall.dws_uv_detail_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
)
    partitioned by(dt string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_uv_detail_day';

insert overwrite table gmall.dws_uv_detail_day
    partition(dt='2019-02-10')
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat
from gmall.dwd_start_log
where dt='2019-02-10'
group by mid_id;

select * from gmall.dws_uv_detail_day limit 1;

-- 2. 周活跃设备明细
drop table if exists gmall.dws_uv_detail_wk;
create external table gmall.dws_uv_detail_wk(
                                                `mid_id` string COMMENT '设备唯一标识',
                                                `user_id` string COMMENT '用户标识',
                                                `version_code` string COMMENT '程序版本号',
                                                `version_name` string COMMENT '程序版本名',
                                                `lang` string COMMENT '系统语言',
                                                `source` string COMMENT '渠道号',
                                                `os` string COMMENT '安卓系统版本',
                                                `area` string COMMENT '区域',
                                                `model` string COMMENT '手机型号',
                                                `brand` string COMMENT '手机品牌',
                                                `sdk_version` string COMMENT 'sdkVersion',
                                                `gmail` string COMMENT 'gmail',
                                                `height_width` string COMMENT '屏幕宽高',
                                                `app_time` string COMMENT '客户端日志产生时的时间',
                                                `network` string COMMENT '网络模式',
                                                `lng` string COMMENT '经度',
                                                `lat` string COMMENT '纬度',
                                                `monday_date` string COMMENT '周一日期',
                                                `sunday_date` string COMMENT  '周日日期'
) COMMENT '活跃用户按周明细'
    PARTITIONED BY (`wk_dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_uv_detail_wk/';

insert overwrite table gmall.dws_uv_detail_wk partition(wk_dt)
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_add(next_day('2019-02-10','MO'),-7),
    date_add(next_day('2019-02-10','MO'),-1),
    concat(date_add( next_day('2019-02-10','MO'),-7), '_' , date_add(next_day('2019-02-10','MO'),-1)
        )
from gmall.dws_uv_detail_day
where dt>=date_add(next_day('2019-02-10','MO'),-7) and dt<=date_add(next_day('2019-02-10','MO'),-1)
group by mid_id;

select * from gmall.dws_uv_detail_wk limit 1;

-- 3. 月活跃设备明细
drop table if exists gmall.dws_uv_detail_mn;

create external table gmall.dws_uv_detail_mn(
                                                `mid_id` string COMMENT '设备唯一标识',
                                                `user_id` string COMMENT '用户标识',
                                                `version_code` string COMMENT '程序版本号',
                                                `version_name` string COMMENT '程序版本名',
                                                `lang` string COMMENT '系统语言',
                                                `source` string COMMENT '渠道号',
                                                `os` string COMMENT '安卓系统版本',
                                                `area` string COMMENT '区域',
                                                `model` string COMMENT '手机型号',
                                                `brand` string COMMENT '手机品牌',
                                                `sdk_version` string COMMENT 'sdkVersion',
                                                `gmail` string COMMENT 'gmail',
                                                `height_width` string COMMENT '屏幕宽高',
                                                `app_time` string COMMENT '客户端日志产生时的时间',
                                                `network` string COMMENT '网络模式',
                                                `lng` string COMMENT '经度',
                                                `lat` string COMMENT '纬度'
) COMMENT '活跃用户按月明细'
    PARTITIONED BY (`mn` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_uv_detail_mn/';

insert overwrite table gmall.dws_uv_detail_mn partition(mn)
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_format('2019-02-10','yyyy-MM')
from gmall.dws_uv_detail_day
where date_format(dt,'yyyy-MM') = date_format('2019-02-10','yyyy-MM')
group by mid_id;

select * from gmall.dws_uv_detail_mn limit 1;


-- 1.日新增设备明细
drop table if exists gmall.dws_new_mid_day;
create external table gmall.dws_new_mid_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`  string  comment '创建时间'
)  COMMENT '每日新增设备信息'
    stored as parquet
    location '/warehouse/gmall/dws/dws_new_mid_day/';


-- 2. 导入数据
insert into table gmall.dws_new_mid_day
select
    ud.mid_id,
    ud.user_id ,
    ud.version_code ,
    ud.version_name ,
    ud.lang ,
    ud.source,
    ud.os,
    ud.area,
    ud.model,
    ud.brand,
    ud.sdk_version,
    ud.gmail,
    ud.height_width,
    ud.app_time,
    ud.network,
    ud.lng,
    ud.lat,
    '2019-02-10'
from gmall.dws_uv_detail_day ud left join gmall.dws_new_mid_day nm on ud.mid_id=nm.mid_id、
-- 新增设备为，新增日志为当天的，和 left join mid is null的数据
where ud.dt='2019-02-10' and nm.mid_id is null;

-- 日新增设备表
drop table if exists gmall.ads_new_mid_count;
create external table gmall.ads_new_mid_count
(
    `create_date` string comment '创建时间' ,
    `new_mid_count` BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备信息数量'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_new_mid_count/';

insert into table gmall.ads_new_mid_count
select
    create_date,
    count(*)
from gmall.dws_new_mid_day
where create_date='2019-02-10'
group by create_date;

-- 留存设备表
drop table if exists gmall.dws_user_retention_day;
create external table gmall.dws_user_retention_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`    string  comment '设备新增时间',
    `retention_day`  int comment '截止当前日期留存天数'
)  COMMENT '每日用户留存情况'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_user_retention_day/'
;

insert overwrite table gmall.dws_user_retention_day
    partition(dt="2019-02-11")
select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    1 retention_day
from gmall.dws_uv_detail_day ud join gmall.dws_new_mid_day nm on ud.mid_id=nm.mid_id
where ud.dt='2019-02-11' and nm.create_date=date_add('2019-02-11',-1);

-- 1，2，3 n天留存
insert overwrite table gmall.dws_user_retention_day
    partition(dt="2019-02-11")
select
    nm.mid_id,
    nm.user_id,
    nm.version_code,
    nm.version_name,
    nm.lang,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    1 retention_day
from gmall.dws_uv_detail_day ud join gmall.dws_new_mid_day nm  on ud.mid_id =nm.mid_id
where ud.dt='2019-02-11' and nm.create_date=date_add('2019-02-11',-1)

union all
select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    2 retention_day
from  gmall.dws_uv_detail_day ud join gmall.dws_new_mid_day nm   on ud.mid_id =nm.mid_id
where ud.dt='2019-02-11' and nm.create_date=date_add('2019-02-11',-2)

union all
select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    3 retention_day
from  gmall.dws_uv_detail_day ud join gmall.dws_new_mid_day nm   on ud.mid_id =nm.mid_id
where ud.dt='2019-02-11' and nm.create_date=date_add('2019-02-11',-3);



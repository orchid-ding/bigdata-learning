#!/bin/bash

#ads 当日、当月、当周活跃设备书
# 定义变量方便修改
APP=gmall
hive=/kfly/install/hive-1.1.0-cdh5.14.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
    if [ -n "$1" ] ;then
    do_date=$1
else
    # shellcheck disable=SC2006
    do_date=`date -d "-1 day" +%F`
fi

sql="
insert overwrite table gmall.dws_user_retention_day
partition(dt='$do_date')
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
where ud.dt='$do_date' and nm.create_date=date_add('$do_date',-1);

insert overwrite table gmall.dws_user_retention_day
partition(dt='$do_date')
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
where ud.dt='$do_date' and nm.create_date=date_add('$do_date',-1)

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
where ud.dt='$do_date' and nm.create_date=date_add('$do_date',-2)

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
where ud.dt='$do_date' and nm.create_date=date_add('$do_date',-3);
"

$hive -e "$sql"

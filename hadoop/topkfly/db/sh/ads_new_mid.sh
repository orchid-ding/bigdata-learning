#!/bin/bash

# 新增设备数
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
    '$do_date'
from gmall.dws_uv_detail_day ud left join gmall.dws_new_mid_day nm on ud.mid_id=nm.mid_id
where ud.dt='$do_date' and nm.mid_id is null;

insert into table gmall.ads_new_mid_count
select
create_date,
count(*)
from gmall.dws_new_mid_day
where create_date='$do_date'
group by create_date;
"

$hive -e "$sql"
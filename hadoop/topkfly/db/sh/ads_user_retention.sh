#!/bin/bash

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
insert into table gmall.ads_user_retention_day_count
select
    create_date,
    retention_day,
    count(*) retention_count
from gmall.dws_user_retention_day
where dt='$do_date'
group by create_date,retention_day;

insert into table gmall.ads_user_retention_day_rate
select
    '$do_date',
    ur.create_date,
    ur.retention_day,
    ur.retention_count,
    nc.new_mid_count,
    ur.retention_count/nc.new_mid_count*100
from gmall.ads_user_retention_day_count ur join gmall.ads_new_mid_count nc
on nc.create_date=ur.create_date;
"

$hive -e "$sql"

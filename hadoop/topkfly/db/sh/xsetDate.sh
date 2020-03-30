#! /bin/bash
log_date=$1
for i in node01 node02 node03
do
        ssh -t $i "sudo date -s $log_date"
done
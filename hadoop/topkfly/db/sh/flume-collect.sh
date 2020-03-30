#! /bin/bash
case $1 in
"start"){
        for i in node03 node02 node01
    do
                        echo "------------启动 $i 采集日志，flume-------------"
                        if [ "node03" = $i ];then
                                ssh $i "source /etc/profile; nohup /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/bin/flume-ng agent -n a1 -c /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/myconf -f /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/myconf/flume-hdfs.conf -Dflume.root.logger=info,console > /dev/null 2 >&1 &"
                                else
                                ssh $i "source /etc/profile; nohup /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/bin/flume-ng agent -n a1 -c /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/myconf -f /kfly/install/apache-flume-1.6.0-cdh5.14.2-bin/myconf/flume-client.conf -Dflume.root.logger=info,console > /dev/null 2 >&1 &"
                                fi
    done
};;
"stop"){
        for i in node01 node02 node03
                do
                        echo "------------停止 $i 采集日志,flume-------------"
                        ssh $i "source /etc/profile; ps -ef | grep flume | grep -v grep |awk '{print \$2}' | xargs kill"
                done
};;
esac
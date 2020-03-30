#! /bin/bash
for i in node01 node02
do
ssh $i "source /etc/profile;java -classpath /kfly/topkfly/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar appclient.AppMain > /kfly/topkfly/run.log"
done
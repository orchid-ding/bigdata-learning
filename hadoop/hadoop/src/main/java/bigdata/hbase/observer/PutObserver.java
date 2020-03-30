package bigdata.hbase.observer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import java.io.IOException;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class PutObserver extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // 连接HBase集群不需要指定HBase主节点的ip地址和端口号,连接到zookeeper
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("observer:copy"));

        List<Cell> cells = put.get("f1".getBytes(), "name".getBytes());
        Cell nameCells = cells.get(0);

        Put put1 = new Put(put.getRow());
        put1.add(nameCells);
        table.put(put1);
        table.close();
        connection.close();

    }
}

package bigdata.hbase.ttl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author dingchuangshi
 * hbase的确界，以及命名空间
 */
public class HBaseTtl {

    private static byte[] TABLE_NAME = "version_hbase".getBytes();

    private static Connection connection;

    public HBaseTtl() throws IOException {
        init();
    }
    public static void main(String[] args) throws IOException {
        // createTable 命名空间下创建表
        HBaseTtl ttl = new HBaseTtl();
        ttl.createTable();
        //insert data
        ttl.insertData();
        // get all version data
        ttl.getRawCell();
        // close
        connection.close();



    }

    /**
     * 初始化连接
     * @throws IOException
     */
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");
        conf.set("fs.fefaultFS","hadoop.hdfs://node01:8020");
        connection = ConnectionFactory.createConnection(conf);
    }

    /**
     * 创建表
     */
    public void createTable() throws IOException {
        Admin admin = connection.getAdmin();
        if(!admin.tableExists(TableName.valueOf(TABLE_NAME))){
            // table
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            // columa family
            HColumnDescriptor column = new HColumnDescriptor("f1");
            // version
            column.setMaxVersions(5);
            column.setMinVersions(3);
            // ttl unit s
            column.setTimeToLive(30);
            table.addFamily(column);
            admin.createTable(table);
        }
        admin.close();
    }

    /**
     * insert data
     * @throws IOException
     */
    public void insertData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        for (int i = 0; i < 6 ; i++) {
            Put put = new Put(("column").getBytes());
            put.addColumn("f1".getBytes(),"col1".getBytes(), System.currentTimeMillis(),Bytes.toBytes("column" + i));
            table.put(put);
        }
    }

    /**
     * get raw cell
     * @throws IOException
     */
    public void getRawCell() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get get = new Get("column".getBytes());
        get.setMaxVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (int i = 0; i < cells.length; i++) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cells[i])));
        }
    }

}

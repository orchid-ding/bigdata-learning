package bigdata.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 数据表初始化
 * @author dingchuangshi
 */
public class WeiBoDataInit {

    public static void main(String[] args) throws IOException {
        WeiBoDataInit weiBoHBaseClient = new WeiBoDataInit();
        weiBoHBaseClient
                // create_namespace 'weibo',creator kfly
                .createNameSpace()
                // create table web bo content
                .createTableWeiBoContent()
                // create table user relation
                .createTableUserRelation()
                // create table receive content email
                .createTableReceiveContentEmail()
                // close connection
                .close();
    }

    /**
     * create 'weibo:receive_content_email','info'
     * @return
     * @throws IOException
     */
    private WeiBoDataInit createTableReceiveContentEmail() throws IOException {
        //获取HTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
        //定义列族名称
        HColumnDescriptor info = new HColumnDescriptor("info");
        info.setMaxVersions(1000);
        info.setMinVersions(1000);
        info.setBlockCacheEnabled(true);
        info.setBlocksize(2048*1024);
        hTableDescriptor.addFamily(info);
        //创建表
        admin.createTable(hTableDescriptor);
        return this;
    }
    /**
     * create 'weibo:relations','attends','fans'
     * @return
     */
    private WeiBoDataInit createTableUserRelation() throws IOException {
        //得到表定义
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATIONS));
        HColumnDescriptor attends = new HColumnDescriptor("attends");
        HColumnDescriptor fans = new HColumnDescriptor("fans");

        attends.setBlocksize(2048*1024);
        attends.setBlockCacheEnabled(true);
        attends.setMinVersions(1);
        attends.setMaxVersions(1);

        fans.setBlocksize(2048*1024);
        fans.setBlockCacheEnabled(true);
        fans.setMinVersions(1);
        fans.setMaxVersions(1);

        hTableDescriptor.addFamily(attends);
        hTableDescriptor.addFamily(fans);
        admin.createTable(hTableDescriptor);
        return this;
    }
    /**
     * create 'weibo:content','info', ...
     * @return
     */
    private WeiBoDataInit createTableWeiBoContent() throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
        HColumnDescriptor column = new HColumnDescriptor("info");
        column.setMaxVersions(1);
        column.setMinVersions(1);
        column.setBlockCacheEnabled(true);
        //设置数据压缩
///     column.setCompressionType(Compression.Algorithm.SNAPPY);
        column.setBlocksize(2048*1024);
        table.addFamily(column);
        admin.createTable(table);
        return this;
    }

    /**
     * create_namespace 'weibo'
     */
    private WeiBoDataInit createNameSpace() throws IOException {
        NamespaceDescriptor namespace = NamespaceDescriptor
                .create(NAME_SPACES)
                .addConfiguration("creator",AUTHOR_NAME).build();
        admin.createNamespace(namespace);
        return this;
    }



    private WeiBoDataInit() throws IOException {
        init();
    }

    /**
     * init connection
     * @throws IOException
     */
    private void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("bigdata.hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");
        conf.set("fs.fefaultFS","bigdata.hadoop.hdfs://node01:8020");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }


    /**
     * close connection
     * @throws IOException
     */
    private void close() throws IOException {
        admin.close();
        connection.close();
    }


    private static Connection connection;
    private static Admin admin;
    private String NAME_SPACES = "weibo";
    private String AUTHOR_NAME = "kfly";

    private byte[] TABLE_CONTENT = "weibo:content".getBytes();
    private byte[] TABLE_RELATIONS = "weibo:relations".getBytes();
    private byte[] TABLE_RECEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();

}

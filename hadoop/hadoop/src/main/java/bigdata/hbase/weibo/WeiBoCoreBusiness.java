package bigdata.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class WeiBoCoreBusiness {

    public static void main(String[] args) throws IOException {
        WeiBoCoreBusiness core = new WeiBoCoreBusiness();
        core
                //publish weibo content
//                .publishWeiBo("004","hello my name is 004")
                // add attends or fans
//                .addAttends("007","004","005","006")
                // cancel attends or fans
//                .attendCancel("007","004")
                .getContent("007")
                .getContent("003")
                .close();
    }

    /**
     * 某个用户获取收件箱表内容
     * 例如A用户刷新微博，拉取他所有关注人的微博内容
     * A 从 weibo:receive_content_email  表当中获取所有关注人的rowkey
     * 通过rowkey从weibo:content表当中获取微博内容
     */
    public WeiBoCoreBusiness getContent(String uid) throws IOException {
        //从 weibo:receive_content_email
        Table table_receive_content_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
        //定义list集合里面存储我们的所有的Get对象，用于下一步的查询
        List<Get>  rowkeysList = new ArrayList<Get>();
        Get get = new Get(uid.getBytes());
        //设置最大版本为5个
        get.setMaxVersions(5);
        Result result = table_receive_content_email.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] rowkeys = CellUtil.cloneValue(cell);
            Get get1 = new Get(rowkeys);
            rowkeysList.add(get1);
        }
        //从weibo:content表当中通过用户id进行查询微博内容
        //table_content内容表
        Table table_content = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //所有查询出来的内容进行打印出来
        Result[] results = table_content.get(rowkeysList);
        for (Result result1 : results) {
            byte[] value = result1.getValue("info".getBytes(), "content".getBytes());
            byte[] row = result1.getRow();
            String rowkey = Bytes.toString(row);
            String[] split = rowkey.split("_");
            System.out.println("\n uid=> " + uid + "\n timestamp => " + split[1] +"\n content => " + Bytes.toString(value));
        }
        return this;
    }

    /**
     * add attends 添加关注
     * 1. add attends  and fans
     * 2. get weibo content
     * @param uid
     * @param attends
     * @return
     */
    private WeiBoCoreBusiness addAttends(String uid,String ...attends) throws IOException {
        // add attends and fans
        // 批量关注attends
        Table relations = connection.getTable(TableName.valueOf(TABLE_RELATIONS));
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < attends.length; i++) {
            String attendsKey = attends[i];
            Put attendPut = new Put(uid.getBytes());
            attendPut.addColumn("attends".getBytes(),attendsKey.getBytes(),attendsKey.getBytes());
            putList.add(attendPut);

            Put fansPut = new Put(attendsKey.getBytes());
            fansPut.addColumn("fans".getBytes(),uid.getBytes(),uid.getBytes());
            putList.add(fansPut);
        }
        relations.put(putList);
        relations.close();

        // get attends wei_bo content insert  to uid
        Table content = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        // wei bo content rowKey list
        List<byte[]> contentRowKeyList = new ArrayList<>();
        Scan scan = new Scan();
        for (String attend : attends) {
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);
            ResultScanner scanner = content.getScanner(scan);
            for (Result result : scanner) {
                byte[] rowKey = result.getRow();
                contentRowKeyList.add(rowKey);
            }
        }
        content.close();

        // if rowKey is not empty , add to wei bo receive email
        Table receive = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
        List<Put> receivePutList = new ArrayList<>();
        contentRowKeyList.forEach(bytes -> {
            String[] rowKeyStr = Bytes.toString(bytes).split("_");
            String attendUid = rowKeyStr[0];
            Long timestamp = Long.parseLong(rowKeyStr[1]);
            Put put = new Put(uid.getBytes());
            put.addColumn("info".getBytes(),attendUid.getBytes(),timestamp,bytes);
            receivePutList.add(put);

        });
        receive.put(receivePutList);
        receive.close();
        return this;
    }

    /**
     * 取消关注 A取消关注 B,C,D这三个用户
     * 其实逻辑与关注B,C,D相反即可
     * 第一步：在weibo:relation关系表当中，在attends列族当中删除B,C,D这三个列
     * 第二步：在weibo:relation关系表当中，在fans列族当中，以B,C,D为rowkey，查找fans列族当中A这个粉丝，给删除掉
     * 第三步：A取消关注B,C,D,在收件箱中，删除取关的人的微博的rowkey
     */
    public WeiBoCoreBusiness attendCancel(String uid,String ...cancelAttends) throws IOException {
        Table table_relations = connection.getTable(TableName.valueOf(TABLE_RELATIONS));

        //移除A关注的B,C,D这三个用户
        for (String cancelAttend : cancelAttends) {
            Delete delete = new Delete(uid.getBytes());
            delete.addColumn("attends".getBytes(),cancelAttend.getBytes());
            table_relations.delete(delete);
        }

        //B,C,D这三个用户移除粉丝A
        for (String cancelAttend : cancelAttends) {
            Delete delete = new Delete(cancelAttend.getBytes());
            delete.addColumn("fans".getBytes(),uid.getBytes());
            table_relations.delete(delete);
        }

        //收件箱表当中  A移除掉B,C,D的信息
        Table table_receive_connection = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));

        for (String cancelAttend : cancelAttends) {
            Delete delete = new Delete(uid.getBytes());
            delete.addColumn("info".getBytes(),cancelAttend.getBytes());
            table_receive_connection.delete(delete);

        }
        table_receive_connection.close();
        table_relations.close();
        return this;
    }

    /**
     * publish weibo content
     * @param uid
     * @param content
     * @return
     * @throws IOException
     */
    private WeiBoCoreBusiness publishWeiBo(String uid,String content) throws IOException {
        Table tableContent = connection.getTable(TableName.valueOf(TABLE_CONTENT));
       // put content row key,content
        String key = uid + "_" + System.currentTimeMillis();
        Put put = new Put(key.getBytes());
        put.addColumn("info".getBytes(),"content".getBytes(),System.currentTimeMillis(),content.getBytes());
        tableContent.put(put);

        // put to relations fans content
        //1. get fans lists
        List<byte[]> allFans = getFansList(uid);
        sendToReceiveEmail(uid,key,allFans);
        return this;
    }

    /**
     * send to wei bo receive email
     * @param uid
     * @param contentKey
     * @param allFans
     */
    private void sendToReceiveEmail(String uid,String contentKey,List<byte[]> allFans) throws IOException {
        Table tableReceiveEmail = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
         List<Put> putList = new ArrayList<>();
        for (byte[] bytes:allFans){
            Put put = new Put(bytes);
            put.addColumn("info".getBytes(),uid.getBytes(),System.currentTimeMillis(),contentKey.getBytes());
            putList.add(put);
        }
        tableReceiveEmail.put(putList);
    }

    /**
     * get all fans
     * @param uid
     * @return
     * @throws IOException
     */
    private List<byte[]> getFansList(String uid) throws IOException {
        List<byte[]> allFans = new ArrayList<>();
        Table tableRelations = connection.getTable(TableName.valueOf(TABLE_RELATIONS));

        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());

        Result result = tableRelations.get(get);
        Cell[] cells = result.rawCells();
        for (int i = 0; i < cells.length; i++) {
            allFans.add(CellUtil.cloneQualifier(cells[0]));
        }
        return allFans;
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

    public WeiBoCoreBusiness() throws IOException {
        init();
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

    private byte[] TABLE_CONTENT = "weibo:content".getBytes();
    private byte[] TABLE_RELATIONS = "weibo:relations".getBytes();
    private byte[] TABLE_RECEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();
}

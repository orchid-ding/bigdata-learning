package bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class HBaseClientTest {

    private static final TableName TABLE_NAME = TableName.valueOf("kfly");
    private static Connection connection;
    private static Table table;

    /**
     * 连接hbase
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        BasicConfigurator.configure();
        Configuration conf = HBaseConfiguration.create();
        // 连接HBase集群不需要指定HBase主节点的ip地址和端口号,连接到zookeeper
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TABLE_NAME);
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws IOException {
        Admin admin = connection.getAdmin();
        // 指定表名称
        HTableDescriptor descriptor = new HTableDescriptor(TABLE_NAME);
        // 指定两个列
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("kf");
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor("ly");
        descriptor.addFamily(columnDescriptor);
        descriptor.addFamily(columnDescriptor1);
        admin.createTable(descriptor);
        admin.close();
    }

    /**
     * 添加数据
     */
    @Test
    public void addData() throws IOException {
        Put row = new Put("001".getBytes());
        row.addColumn("kf".getBytes(),"name".getBytes(),"Job".getBytes());
        table.put(row);
    }

    /**
     * 查询对象
     */
    @Test
    public void get() throws IOException {
        Get get = new Get("001".getBytes());
        Result result = table.get(get);
        result.listCells().forEach(action->{
            System.out.println("columns --> " + Bytes.toString(CellUtil.cloneFamily(action)));
            System.out.println("columns --> " + Bytes.toString(CellUtil.cloneQualifier(action)));
            System.out.println("columns --> " + Bytes.toString(CellUtil.cloneRow(action)));
            System.out.println("columns --> " + Bytes.toString(CellUtil.cloneValue(action)));
        });

    }


    /**
     * 查询对象
     */
    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        printlnResultScanner(resultScanner);

    }


    // start 比较过滤器
    /**
     * rowKey 过滤器
     * @throws IOException
     */
    @Test
    public void rowFilter() throws IOException {
        BinaryComparator comparator = new BinaryComparator("001".getBytes());
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,comparator);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        printlnResultScanner(resultScanner);
    }

    /**
     *列簇过滤器
     */
    @Test
    public void columnsFilter() throws IOException {
        SubstringComparator comparator = new SubstringComparator("ly");
        Scan scan = new Scan();
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,comparator);
        scan.setFilter(familyFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        printlnResultScanner(resultScanner);
    }


    /**
     * 列名过滤器
     */
    @Test
    public void qualifierFilter() throws IOException {
        SubstringComparator comparator = new SubstringComparator("name");
        Scan scan = new Scan();
        QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,comparator);
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        printlnResultScanner(resultScanner);
    }


    /**
     * 值过滤器，针对所有的列
     */
    @Test
    public void valueFilter() throws IOException {
        SubstringComparator comparator = new SubstringComparator("b");
        Scan scan = new Scan();
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,comparator);
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        printlnResultScanner(resultScanner);
    }

    // end 比较过滤器

    // start 专用过滤器

    /**
     * 单列值过滤器
     * @throws IOException
     */
    @Test
    public void singleColumnValueFilter() throws IOException {
        //查询 f1  列族 name  列  值为刘备的数据
        Scan scan = new Scan();
        //单列值过滤器，过滤  f1 列族  name  列  值为刘备的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("kf".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "Job".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlnResultScanner(scanner);
    }

    /**
     * 单列值过滤器，排除过滤器
     * @throws IOException
     */
    @Test
    public void singleColumnValueExcludeFilter() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter
                = new SingleColumnValueExcludeFilter(
                        "kf".getBytes(), "name".getBytes(),
                CompareFilter.CompareOp.EQUAL, "tom".getBytes());
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlnResultScanner(scanner);
    }

    /**
     * HBase当中的分页
     */
    @Test
    public void hBasePageFilter() throws IOException {
        int pageNum= 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if(pageNum == 1 ){
            //获取第一页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            //使用分页过滤器来实现数据的分页
            PageFilter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            printlnResultScanner(scanner);
        }else{
            String  startRow = "";
            //扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            //设置一步往前扫描多少条数据
            scan.setMaxResultSize(scanDatas);
            PageFilter filter = new PageFilter(scanDatas);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                //获取rowkey
                byte[] row = result.getRow();
                //最后一次startRow的值就是0005
                //循环遍历我们多有获取到的数据的rowkey
                startRow= Bytes.toString(row);
                //最后一条数据的rowkey就是我们需要的起始的rowkey
            }
            //获取第三页的数据
            scan.setStartRow(startRow.getBytes());
            //设置我们扫描多少条数据
            scan.setMaxResultSize(pageSize);
            PageFilter filter1 = new PageFilter(pageSize);
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printlnResultScanner(scanner1);
        }
    }

    /**
     * 查询  f1 列族  name  为刘备数据值
     * 并且rowkey 前缀以  00开头数据
     */
    @Test
    public  void filterList() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printlnResultScanner(scanner);
    }

    /**
     * 删除数据
     */
    @Test
    public  void  deleteData() throws IOException {
        Delete delete = new Delete("0003".getBytes());
        table.delete(delete);
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {
        //获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        //删除一张表之前，需要先禁用表
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
    }

    /**
     * 查询rowkey前缀以  00开头的所有的数据
     */
    @Test
    public  void  prefixFilter() throws IOException {
        Scan scan = new Scan();
        // 过滤rowkey以  00开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("r".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlnResultScanner(scanner);
    }
    // end 专用过滤器
    /**
     * 关闭连接
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }


    /**
     * 打印数据
     * @param resultScanner
     */
    public void printlnResultScanner(ResultScanner resultScanner){
        resultScanner.forEach(result -> {
            result.listCells().forEach(action->{
                System.out.println("columns --> " + Bytes.toString(CellUtil.cloneFamily(action)));
                System.out.println("columns --> " + Bytes.toString(CellUtil.cloneQualifier(action)));
                System.out.println("columns --> " + Bytes.toString(CellUtil.cloneRow(action)));
                System.out.println("columns --> " + Bytes.toString(CellUtil.cloneValue(action)));
            });
        });
    }
}

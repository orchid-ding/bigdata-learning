package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class CreateHbaseTableInit {
    public static void main(String[] args) throws Exception {
        Connection hbaseConn = HbaseTools.getHbaseConn();
        CreateHbaseTableInit createHbaseTableInit = new CreateHbaseTableInit();
        /**
         * syn.table.order_info="order_info"
         * syn.table.renter_info="renter_info"
         * syn.table.driver_info="driver_info"
         * syn.table.opt_alliance_business="opt_alliance_business"
         */
        String[] tableNames = new String[]{"order_info","renter_info","driver_info","opt_alliance_business"};
        for (String tableName : tableNames) {
            createHbaseTableInit.createTable(hbaseConn,Constants.DEFAULT_REGION_NUM,tableName);
        }
        hbaseConn.close();
    }


    public void createTable(Connection connection,int regionNum,String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if(admin.tableExists(TableName.valueOf(tableName))){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
         /*  //HBase自带的分区工具类，自动帮我们进行分区
            //获取到的是16进制的字符串
            RegionSplitter.HexStringSplit spliter = new RegionSplitter.HexStringSplit();
            byte[][] split = spliter.split(8);
            //适合rowkey经过hash或者md5之后的字符串
            RegionSplitter.UniformSplit uniformSplit = new RegionSplitter.UniformSplit();
            byte[][] split1 = uniformSplit.split(8);*/
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Constants.DEFAULT_DB_FAMILY);
        hTableDescriptor.addFamily(hColumnDescriptor);
        byte[][] splitKey = getSplitKey(regionNum);
        admin.createTable(hTableDescriptor,splitKey);
        admin.close();
    }

    public byte[][] getSplitKey(int regionNum){
        byte[][] byteNum = new byte[regionNum][];
        for(int i =0;i<regionNum;i++){
            String leftPad = StringUtils.leftPad(i+"",4,"0");
            byteNum[i] = Bytes.toBytes(leftPad + "|");
        }
        return byteNum;
    }
}

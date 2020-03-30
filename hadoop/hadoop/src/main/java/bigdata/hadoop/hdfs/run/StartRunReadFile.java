package bigdata.hadoop.hdfs.run;

import bigdata.hadoop.hdfs.core.HadoopFileSystem;

/**
 * 2.读取hdfs上的文件
 * @author dingchuangshi
 */
public class StartRunReadFile {

    /**
     * 读取文件
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(HadoopFileSystem.readFileFromFileSystem(args[0]));
    }
}

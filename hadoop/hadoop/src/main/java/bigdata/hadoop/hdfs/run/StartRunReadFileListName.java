package bigdata.hadoop.hdfs.run;

import bigdata.hadoop.hdfs.core.HadoopFileSystem;

/**
 *  3.列出某一个文件夹下的所有文件
 * @author dingchuangshi
 */
public class StartRunReadFileListName {

    public static void main(String[] args) {
        System.out.println(HadoopFileSystem.listAllFileChildren(args[0]));
    }
}

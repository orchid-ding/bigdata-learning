package bigdata.hadoop.hdfs.run;

import bigdata.hadoop.hdfs.core.HadoopFileSystem;

/**
 *  4. 列出多级目录名称和目录下的文件名称
 * @author dingchuangshi
 */
public class StartRunReadAllListName {

    public static void main(String[] args) {
        System.out.println(HadoopFileSystem.listAllChildren(args[0]));
    }
}

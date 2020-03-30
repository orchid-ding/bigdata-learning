package bigdata.hadoop.hdfs.run;

import bigdata.hadoop.hdfs.core.HadoopFileSystem;

/**
 * 1.向hdfs中,上传一个文本文件
 * @author dingchuangshi
 */
public class StartRunUploadFile {

    /**
     *
     * @param args
     *  0 ： 源文件
     *  1 ： 目标文件系统
     */
    public static void main(String[] args) {
        HadoopFileSystem.uploadFileToFileSystem(args[0],args[1]);
    }
}

package bigdata.hadoop.hdfs.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;
import java.net.URI;

/**
 * hadoop 文件系统操作
 * @author dingchuangshi
 */
public class HadoopFileSystem {

    /**
     * 上传文件到服务器
     *  传递参数
     *  args[0] 本地文件路径
     *  args[1] hdoop文件系统 路径
     */
    public static void uploadFileToFileSystem(String source,String targetUrl){
        System.out.println("文件地址：" + source);
        System.out.println("目标服务器：" + targetUrl);
        InputStream inputStreamSourceFile = null;
        try {
            // 获取文件输入流
            inputStreamSourceFile = new BufferedInputStream(new FileInputStream(source));
            // HDFS 读写配置文件
            Configuration configuration = new Configuration();
            // 通过url 返回文件系统实例
            FileSystem fileSystem = FileSystem.get(URI.create(targetUrl),configuration);
            //调用Filesystem的create方法返回的是FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            // 获取文件系用的输出流
            OutputStream outputStreamTarget = fileSystem.create(new Path(targetUrl));

            // 将文件输入流，写入输入流
            IOUtils.copyBytes(inputStreamSourceFile,outputStreamTarget,4069,true);
            System.out.println("上传成功");
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 上传压缩文件到服务器
     *  传递参数
     *  args[0] 本地文件路径
     *  args[1] hdoop文件系统 路径
     */
    public static void uploadFileZipToFileSystem(String source,String targetUrl){
        System.out.println("文件地址：" + source);
        System.out.println("目标服务器：" + targetUrl);
        InputStream inputStreamSourceFile = null;

        try {
            // 获取文件输入流
            inputStreamSourceFile = new BufferedInputStream(new FileInputStream(source));
            // HDFS 读写配置文件
            Configuration configuration = new Configuration();
            // 压缩类型
            BZip2Codec codec = new BZip2Codec();
            codec.setConf(configuration);
            // 通过url 返回文件系统实例
            FileSystem fileSystem = FileSystem.get(URI.create(targetUrl),configuration);
            //调用Filesystem的create方法返回的是FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            // 获取文件系用的输出流
            OutputStream outputStreamTarget = fileSystem.create(new Path(targetUrl));
            // 对输出流进行压缩
            CompressionOutputStream compressionOut = codec.createOutputStream(outputStreamTarget);
            // 将文件输入流，写入输入流
            IOUtils.copyBytes(inputStreamSourceFile,compressionOut,4069,true);
            System.out.println("上传成功");
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 从文件系统中读取文件
     * @param source 需要读取的文件
     * @return 读取文件内容
     */
    public static String readFileFromFileSystem(String source){
        String result = null;
        try {
            // HDFS 读写文件配置
            Configuration configuration = new Configuration();
            // HDFS文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(source),configuration);
            // 文件输入流，用于读取文件
            InputStream inputStream = fileSystem.open(new Path(source));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            result = readBufferReader(bufferedReader).toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 列出当前目录下所有字文件名称
     * @param source
     * @return
     */
    public static String listAllFileChildren(String source){
        StringBuffer stringBuffer = new StringBuffer();
        try {
            // HDFS 读写文件配置
            Configuration configuration = new Configuration();
            // HDFS文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(source),configuration);
            // recursive 继续深入遍历
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(source),true);
            while (iterator.hasNext()){
                LocatedFileStatus fileStatus = iterator.next();
                stringBuffer.append(fileStatus.getPath() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringBuffer.toString();
    }

    /**
     * 递归列出当前目录下所有目录和文件名称
     * @param source
     * @return
     */
    public static String listAllChildren(String source){
        StringBuffer stringBuffer = new StringBuffer();
        try {
            // HDFS 读写文件配置
            Configuration configuration = new Configuration();
            // HDFS文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(source),configuration);
            list(stringBuffer,fileSystem,new Path(source));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringBuffer.toString();
    }

    /**
     * 递归目录和文件
     * @param stringBuffer  文件目录名称集合
     * @param fileSystem  hadoop.hdfs 文件系统
     * @param source path 路径
     * @throws IOException
     */
    private static void list(StringBuffer stringBuffer,FileSystem fileSystem, Path source) throws IOException {
        FileStatus[] iterator = fileSystem.listStatus(source);
        for (FileStatus status:iterator) {
            stringBuffer.append(status.getPath() + "\n");
            if(status.isDirectory()){
                list(stringBuffer,fileSystem,status.getPath());
            }

        }
    }


    /**
     * 获取内容
     * @param br
     * @return
     */
    private static StringBuffer readBufferReader(BufferedReader br) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();
        String temp = "";
        while ((temp = br.readLine()) != null){
            stringBuffer.append(temp);
        }
        return stringBuffer;
    }
}

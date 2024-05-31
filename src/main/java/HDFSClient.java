import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URI;
import static java.lang.System.out;

public class HDFSClient {
    private static FileSystem hdfs;
    /**
     * 连接HDFS
     */
    static{
        try {
            URI uri = new URI("hdfs://192.168.137.129:9820");
            Configuration conf = new Configuration();
            String user = "root";
            hdfs = FileSystem.get(uri,conf,user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建指定目录
     * @param path
     * @throws IOException
     */
    public static void mkdir(String path) throws IOException{
        hdfs.mkdirs(new Path(path));
    }

    /**
     * 删除指定目录
     * @param path
     * @throws IOException
     */
    public static void delete(String path) throws IOException {
        hdfs.delete(new Path(path));
    }

}

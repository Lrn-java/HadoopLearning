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
    static {
        try {
            URI uri = new URI("hdfs://192.168.137.129:9820");
            Configuration conf = new Configuration();
            String user = "root";
            hdfs = FileSystem.get(uri, conf, user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建指定目录
     *
     * @param path
     * @throws IOException
     */
    public static boolean mkdir(String path) throws IOException {
        if (hdfs.exists(new Path(path))){
            out.println("该目录存在");

        }else{
            boolean access = hdfs.mkdirs(new Path(path));
            out.println("创建" + (access ? "成功" : "失败") + ":" + path);
        }
        return false;
    }

    /**
     * 删除指定目录
     *
     * @param path
     * @throws IOException
     */
    public static boolean delete(String path) throws IOException {
        if (hdfs.exists(new Path(path)));
        return false;
    }
}


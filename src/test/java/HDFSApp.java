import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;


/**
 * Created by hello on 2018-05-11.
 */
public class HDFSApp {

    public static final String HDFS_PATH ="hdfs://192.168.91.127:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream fs=fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        fs.write("hello haddop".getBytes());
        fs.flush();
        fs.close();
    }

    /**
     * 查看文件内容
    */
    @Test
    public void cat() throws  Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/b.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }

    /**
     * 重命名文件
     * @throws Exception
     */

    @Test
    public void rename() throws Exception{
        fileSystem.rename(new Path("/hdfsapi/test/a.txt"),new Path("/hdfsapi/test/b.txt"));
    }

    /**
     * 显示文件目录
     * @throws Exception
     */
    @Test
    public void catgory() throws Exception{
        FileStatus[] fs=fileSystem.listStatus(new Path("/hdfsapi/test/dalei.json"));
        for(FileStatus fileStatus : fs){
            String isDir = fileStatus.isDirectory() ? "文件夹":"文件";
            short replication =fileStatus.getReplication();
            long leng=fileStatus.getLen();
            Path path = fileStatus.getPath();
            System.out.println(isDir+"\t"+replication+"\t"+leng+"\t"+path);
        }
    }


    /**
     *
     * 上传文件到hdfs
     * @throws Exception
     */
    @Test
    public void upload() throws Exception{
        Path localPath = new Path("E:\\train.json");
        Path hdfsPath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 带进度条的上传
     * @throws Exception
     */
    @Test
    public void uploadWithProgress() throws Exception{
        Path localPath = new Path("E:\\train.json");
        Path hdfsPath = new Path("/hdfsapi/test/");
        Progressable progress = new Progressable() {
            public void progress() {
                System.out.print(">");
            }
        };
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\train.json")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/dalei.json"),progress);
        IOUtils.copyBytes(in,output,4096);
    }


    /**
     *从hdfs上下载文件
     * @throws Exception
     */
    @Test
    public void download() throws Exception{
        Path localPath = new Path("D:\\hi.txt");
        Path hdfsFile = new Path("/hdfsapi/test/dalei.json");
        fileSystem.copyToLocalFile(hdfsFile,localPath);

    }


    @Before
    public void setUp() throws Exception{
        System.out.println("hadoop setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"root");
    }
    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("hadoop tearDown");
    }

}

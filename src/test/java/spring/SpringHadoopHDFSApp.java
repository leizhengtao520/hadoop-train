package spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 使用spring-hadoop来访问hdfs文件系统
 * Created by hello on 2018-05-16.
 */
public class SpringHadoopHDFSApp {

    private ApplicationContext ctx ;

    private FileSystem fileSystem;


    @Test
    public void testMkdir() throws Exception{
        fileSystem.mkdirs(new Path("/springhdfs/"));
    }

    /**
     * 读取hdfs上的文件
     */
    @Test
    public void textHdfs() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/test/a.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }



    @Before
    public void setUp(){
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown() throws Exception{
        ctx = null;
        fileSystem.close();
    }
}

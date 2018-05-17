package spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * 使用springboot的方式访问hdfs
 *
 */
@SpringBootApplication
public class SpringBootHdfsApp implements CommandLineRunner{

    @Autowired
    FsShell fsShell;

    public void run(String... strings) throws Exception {

        for( FileStatus fileStatus : fsShell.lsr("/hdfsapi")){
            String s=fileStatus.getPath().toString();
            System.out.println(s);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHdfsApp.class,args);
    }
}

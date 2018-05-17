package hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * 使用mr 开发wordcount
 * Created by hello on 2018-05-15.
 */
public class WordCountApp {


    /**
     * Map 读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接收到每一行数据
            String line = value.toString();
            //按照指定分隔符拆分
            String[] words = line.split("\t");

            for(String s : words){
                //通过上下文对map输出
                context.write(new Text(s),one);
            }

        }
    }

    /**
     * Reducer归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0 ;
            for(LongWritable value : values){
                sum += value.get();
            }
            //最终输出总数
            context.write(key,new LongWritable(sum));
        }
    }

    /**
     * driver：封装mr作业的所有信息
     */
    public static void main(String[] args) throws Exception{


        String HDFS_PATH ="hdfs://192.168.91.127:8020";

        //创建configuration
        Configuration configuration = new Configuration();

        //远程调试hadoop
        System.setProperty("hadoop.home.dir","D:\\KDR\\hadoop-2.6.0-cdh5.7.0");
        configuration.set("fs.defaultFS",HDFS_PATH);
        configuration.set("yarn.resourcemanager.hostname",HDFS_PATH);

        //创建job
        Job job = Job.getInstance(configuration,"worconut");
        //设置job处理类
        job.setJarByClass(WordCountApp.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job,new Path("/test/a.txt"));

        //设置map相关的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //通过job设置combiner处理类，其实逻辑上和我们的reduce是一样的
        job.setCombinerClass(MyReducer.class);

        //设置作业处理的输出路径
        Path outPath = new Path("/out/");
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
            System.out.println("outPath is exist,but i has deleted");
        }
        FileOutputFormat.setOutputPath(job,new Path("/out/"));

        System.exit(job.waitForCompletion(true)? 0 : 1);

    }
}

package com.pan.mapreduce.partitionerAndWritableComparable;

import com.pan.mapreduce.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/31 20:20
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //        1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//        2.设置jar包路径
        job.setJarByClass(FlowDriver.class);
//        3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);
//        4.设置map输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
//        5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

//        6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\workspace\\hadoopDemo\\MapReduceDemo\\src\\main\\resources\\result\\phone\\43\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\workspace\\hadoopDemo\\MapReduceDemo\\src\\main\\resources\\result\\phone\\" + DateUtil.now()));
//        7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

package com.pan.mapreduce.combiner;

import com.pan.mapreduce.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/22 16:04
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//        2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);
//        3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
//        4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountReducer.class);

//        6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\workspace\\hadoopDemo\\MapReduceDemo\\src\\main\\resources\\word.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\workspace\\hadoopDemo\\MapReduceDemo\\src\\main\\resources\\result\\word\\" + DateUtil.now()));
//        7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}

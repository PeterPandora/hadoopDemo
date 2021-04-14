package com.pan.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/22 16:03
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}

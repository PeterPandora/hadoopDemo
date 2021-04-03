package com.pan.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/22 16:04
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Object value : values) {
            sum += Integer.valueOf(value.toString());
        }

        IntWritable result = new IntWritable(sum);

        context.write(key, result);
    }
}

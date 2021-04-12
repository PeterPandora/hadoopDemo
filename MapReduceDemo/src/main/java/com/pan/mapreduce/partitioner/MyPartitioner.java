package com.pan.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 潘聪
 * @description
 * @date 2021/4/12 22:10
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        int partition;
        if ("135".equals(prePhone)) {
            partition = 0;
        } else if ("136".equals(prePhone)) {
            partition = 1;
        } else if ("137".equals(prePhone)) {
            partition = 2;
        } else if ("138".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}

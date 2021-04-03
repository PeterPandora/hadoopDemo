package com.pan.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/31 20:10
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    private Text outKey = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUp = 0L;
        long totalDown = 0L;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        outKey.set(key);
        flowBean.setUpFlow(totalUp);
        flowBean.setDownFlow(totalDown);
        flowBean.setSumFlow();

        context.write(outKey, flowBean);

    }
}

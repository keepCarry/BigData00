package com.leopo.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowStatisticsMapper extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{

        String[] strs = value.toString().split("\t",-1);
        if(strs.length != 11){
            return;
        }
        FlowBean flowBean = new FlowBean(Long.valueOf(strs[8]),Long.valueOf(strs[9]));
        context.write(new Text(strs[0].trim()),new Text(flowBean.toString()));
    }
}

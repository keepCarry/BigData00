package com.leopo.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowStatisticsReducer extends Reducer<Text,Text,Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow = 0;
        for(Text value : values){
            String[] strs = value.toString().split("\t",-1);
            upFlow += Long.valueOf(strs[0]);
            downFlow += Long.valueOf(strs[1]);
            sumFlow += Long.valueOf(strs[2]);
        }
        context.write(key,new Text(upFlow + "\t" + downFlow + "\t" + sumFlow));
    }
}

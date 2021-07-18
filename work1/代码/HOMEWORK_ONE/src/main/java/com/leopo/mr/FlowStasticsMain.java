package com.leopo.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowStasticsMain {

    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        Job job = Job.getInstance(config,"liupeng");
        job.setJarByClass(FlowStasticsMain.class);
        job.setMapperClass(FlowStatisticsMapper.class);
        job.setReducerClass(FlowStatisticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path("/user/student/liupeng/homework1/input/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job,new Path("/user/student/liupeng/homework1/output/"));
        System.exit(job.waitForCompletion(true) ? 0:1 );

    }
}

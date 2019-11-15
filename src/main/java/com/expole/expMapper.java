package com.expole;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by fangyitao on 2019/11/13.
 */
public class expMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}

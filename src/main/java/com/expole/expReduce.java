package com.expole;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by fangyitao on 2019/11/13.
 */
public class expReduce extends TableReducer<Text,NullWritable,NullWritable> {

    protected void reduce(Text key,Iterable<NullWritable> values,Context context){

    }
}

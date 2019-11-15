package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 *
 * @description:
 * @Author:bella
 * @Date:2019/11/120:54
 * @Version:
 **/
public class Hbase2Hdfs implements Tool{

    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "master:2181,slave1:2181,slave2:2181";
    private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    private final static String HDFS_CONNECT_VALUE = "hdfs://master:9000/";
    private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
    private final static String MAPREDUCE_CONNECT_VALUE = "yarn";



    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(configuration, "hbase2hdfs");
        job.setJarByClass(Hbase2Hdfs.class);

//        TableMapReduceUtil.initTableMapperJob(
//                sourceTable, // 输入表;
//                scan, // 扫描表配置;
//                MyMapper.class, // mapper类
//                Text.class, // mapper输出Key
//                IntWritable.class, // mapper输出Value
//                job);

        TableMapReduceUtil.initTableMapperJob("myns:user_info", getScan(), HbaseMapper.class,
                Text.class, NullWritable.class, job);


//        TableMapReduceUtil.initTableReducerJob(
//                targetTable, // 输出表
//                MyTableReducer.class, // reducer类
//                job);


        FileOutputFormat.setOutputPath(job,new Path("/hbaseout/04"));
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    public void setConf(Configuration configuration) {
        configuration.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // 设置连接的hbase
        configuration.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // 设置连接的hadoop
        configuration.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE); // 设置使用的mr运行平台
        this.configuration = configuration;
    }


    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new Hbase2Hdfs(), args);
    }

    private static Scan getScan() {
        Scan scan = new Scan();
        scan.setCaching(500); // 在MR作业中，适当设置该值可提升性能
        scan.setCacheBlocks(false); // 在MR作业中，应总为false
        return scan;
    }
}


class HbaseMapper extends TableMapper<Text, NullWritable> {
    private Text k = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //0. 定义字符串存放最终结果
        StringBuffer sb = new StringBuffer();
        //1. 获取扫描器进行扫描解析
        CellScanner cellScanner = value.cellScanner();
        //2. 推进
        while (cellScanner.advance()) {
            //3. 获取当前单元格
            Cell cell = cellScanner.current();
            //4. 拼接字符串
            sb.append(new String(CellUtil.cloneQualifier(cell)));
            sb.append(":");
            sb.append(new String(CellUtil.cloneValue(cell)));
            sb.append("\t");
        }
        //5. 写出
        k.set(sb.toString());
        context.write(k, NullWritable.get());
    }
}

//class HbaseReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
//    @Override
//    public void reduce(Text key, Iterable<IntWritable> values, Context context)
//            throws IOException, InterruptedException {
//        int i = 0;
//        for (IntWritable val : values) {
//            i += val.get();
//        }
//        Put put = new Put(Bytes.toBytes(key.toString()));
//        put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes("信息"));
//        context.write(null, put);
//    }
//}
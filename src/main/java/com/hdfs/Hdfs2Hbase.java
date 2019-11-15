package com.hdfs;

import com.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/120:55
 * @Version:
 **/
public class Hdfs2Hbase implements Tool{

    //1. �������ö���
    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "master:2181,slave1:2181,slave2:2181";
    //private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    //private final static String HDFS_CONNECT_VALUE = "hdfs://qf/";
    //private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
    //private final static String MAPREDUCE_CONNECT_VALUE = "yarn";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Hdfs2Hbase.class);
        job.setMapperClass(HBaseMapper.class);
        job.setReducerClass(HBaseReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        String tablename = "user_infomation";
        createTable(tablename);
        FileInputFormat.setInputPaths(job,new Path("D://information.txt"));

        TableMapReduceUtil.initTableReducerJob(tablename,HBaseReducer.class,job);

        return job.waitForCompletion(true)?1:0;
    }

    public void setConf(Configuration configuration) {
        configuration.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // �������ӵ�hbase
        //configuration.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // �������ӵ�hadoop
        //configuration.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE); // ����ʹ�õ�mr����ƽ̨
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }


    private void createTable(String tablename) {
        //1. ��ȡadmin����
        Admin admin = HbaseUtils.getAdmin();
        //2.
        try {
            boolean isExist = admin.tableExists(TableName.valueOf(tablename));
            if(isExist) {
                admin.disableTable(TableName.valueOf(tablename));
                admin.deleteTable(TableName.valueOf(tablename));
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
            HColumnDescriptor columnDescriptor2 = new HColumnDescriptor("age_info");
            columnDescriptor2.setBloomFilterType(BloomType.ROW);
            columnDescriptor2.setVersions(1, 3);
            tableDescriptor.addFamily(columnDescriptor2);
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            HbaseUtils.close();
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new Hdfs2Hbase(), args);
    }
}

class HBaseMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    Text text = new Text();
    LongWritable lw = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] datas = line.split(",");
        text.set(datas[0]);
        lw.set(Long.parseLong(datas[1]));
        context.write(text,lw);
    }
}

class HBaseReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1. ������
        long count = 0l;
        //2. ����
        Iterator<LongWritable> iterator = values.iterator();
        //3. ���һ��Ҫ�ǿ����޸�hbase�Ķ���put��delete
        Put put = new Put(Bytes.toBytes(key.toString()));
        String value = values.iterator().next().toString();
        //4. �������д��put����
        put.addColumn(Bytes.toBytes("age_info"), Bytes.toBytes("age"), Bytes.toBytes(value));
        //5. д
        context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
    }
}
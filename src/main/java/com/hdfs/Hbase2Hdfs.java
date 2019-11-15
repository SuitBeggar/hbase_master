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
//                sourceTable, // �����;
//                scan, // ɨ�������;
//                MyMapper.class, // mapper��
//                Text.class, // mapper���Key
//                IntWritable.class, // mapper���Value
//                job);

        TableMapReduceUtil.initTableMapperJob("myns:user_info", getScan(), HbaseMapper.class,
                Text.class, NullWritable.class, job);


//        TableMapReduceUtil.initTableReducerJob(
//                targetTable, // �����
//                MyTableReducer.class, // reducer��
//                job);


        FileOutputFormat.setOutputPath(job,new Path("/hbaseout/04"));
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    public void setConf(Configuration configuration) {
        configuration.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // �������ӵ�hbase
        configuration.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // �������ӵ�hadoop
        configuration.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE); // ����ʹ�õ�mr����ƽ̨
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
        scan.setCaching(500); // ��MR��ҵ�У��ʵ����ø�ֵ����������
        scan.setCacheBlocks(false); // ��MR��ҵ�У�Ӧ��Ϊfalse
        return scan;
    }
}


class HbaseMapper extends TableMapper<Text, NullWritable> {
    private Text k = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //0. �����ַ���������ս��
        StringBuffer sb = new StringBuffer();
        //1. ��ȡɨ��������ɨ�����
        CellScanner cellScanner = value.cellScanner();
        //2. �ƽ�
        while (cellScanner.advance()) {
            //3. ��ȡ��ǰ��Ԫ��
            Cell cell = cellScanner.current();
            //4. ƴ���ַ���
            sb.append(new String(CellUtil.cloneQualifier(cell)));
            sb.append(":");
            sb.append(new String(CellUtil.cloneValue(cell)));
            sb.append("\t");
        }
        //5. д��
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
//        put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes("��Ϣ"));
//        context.write(null, put);
//    }
//}
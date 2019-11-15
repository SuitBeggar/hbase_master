package com.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/1123:28
 * @Version:
 **/
public class HbaseUtils {
    private static Configuration configuration = null;
    private static ExecutorService executor = null;
    private static Admin admin = null;
    private static Connection connection = null;

    static {
        try {
            initHbase();
            admin = initHbase().getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //连接集群
    public static Connection initHbase() throws IOException {

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "127.0.0.1:60000");
        executor = Executors.newFixedThreadPool(20);
        connection = ConnectionFactory.createConnection(configuration,executor);
        return connection;
    }

    /**
     * 判断表是否存在
     * @param tableNmae
     * @return
     */
    public static boolean tableExists(String tableNmae){
        try {
            TableName tableName = TableName.valueOf(tableNmae);
            if(admin.tableExists(tableName)){
                return true;
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 创建表
     * @param tableNmae
     * @param cols
     * @throws IOException
     */
    public static void createTable(String tableNmae, String[] cols){
        try {
            TableName tableName = TableName.valueOf(tableNmae);
            if (admin.tableExists(tableName)) {
                System.out.println("表已存在！");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String col : cols) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * 描述表
     * @param tableName
     * @throws Exception
     */
    public static void descTable(String tableName) throws Exception {
        Table table= initHbase().getTable(TableName.valueOf(tableName));
        HTableDescriptor desc =table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();

        for(HColumnDescriptor t:columnFamilies){
            System.out.println(Bytes.toString(t.getName()));
        }
        close();
    }

    /**
     * 删除表
     * @param tableName
     */
    public static void deleteTable(String tableName){

        try {
            TableName tablename = TableName.valueOf(tableName);
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * 替换该表tableName的所有列簇
     * @param tableName
     * @param familyNameList
     */
    public static void modifyTable(String tableName,List<String> familyNameList) {
        try {
            TableName tablename = TableName.valueOf(tableName);
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

            for (String familyName:familyNameList) {
                htd.addFamily(new HColumnDescriptor(Bytes.toBytes(familyName)));
            }
            admin.modifyTable(tablename, htd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        close();
        System.out.println("修改成功");

    }

    /**
     * 删除该表tableName当中的特定的列
     * @param tableName
     * @param familyName
     */
    public static void deleteColumn(String tableName,String familyName) {
        try {
            admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(familyName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
        System.out.println("删除成功");

    }

    /**
     * 获取所以表
     * @throws Exception
     */
    public static void getAllTables() throws Exception {

        TableName[] tableNames = admin.listTableNames();
        for(int i=0;i<tableNames.length;i++){
            System.out.println(tableNames[i].getName());
        }
    }

    /**
     * 更新数据  插入数据
     * @param tableName
     * @param rowKey
     * @param familyName 列簇
     * @param columnName 字段
     * @param value 值
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String familyName, String columnName, String value) {

        Table table= null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            Put put=new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * 根据rowkey 查询
     * @param tableName
     * @param rowKey
     * @return
     */
    public Result getResult(String tableName, String rowKey){
        Result result= null;
        try {
            Get get=new Get(Bytes.toBytes(rowKey));
            Table table= initHbase().getTable(TableName.valueOf(tableName));
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        printResult(result);
        close();
        return result;
    }


    /**
     *查询指定的某列
     * @param tableName
     * @param rowKey
     * @param familyName 列簇
     * @param columnName 字段
     * @return
     */
    public Result getResult(String tableName, String rowKey, String familyName, String columnName){
        Get get=new Get(Bytes.toBytes(rowKey));
        Table table= null;
        Result result = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
            result=table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }

        printResult(result);
        close();
        return result;
    }


    /**
     * 遍历查询表（全表扫描）
     * @param tableName
     * @return
     */
    public ResultScanner getResultScann(String tableName){

        Scan scan=new Scan();
        ResultScanner rs =null;
        Table table = null;
        try{
            table = initHbase().getTable(TableName.valueOf(tableName));
            rs=table.getScanner(scan);
            for(Result r: rs){
                printResult(r);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally{
            rs.close();
        }
        close();
        return rs;
    }

    /**
     * 遍历查询表
     * @param tableName
     * @param scan
     * @return
     */
    public ResultScanner getResultScann(String tableName, Scan scan){

        ResultScanner rs =null;
        Table table= null;
        Result result = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            rs=table.getScanner(scan);
            for(Result r: rs){
                printResult(result);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally{
            rs.close();
        }
        close();
        return rs;
    }

    /**
     * 查询某列数据的某个版本
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param versions
     * @return
     */
    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
                                     int versions) {
        Result result = null;
        Table table = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            Get get =new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            get.setMaxVersions(versions);
            result=table.get(get);

            printResult(result);
        }catch (IOException e){
            e.printStackTrace();
        }
        close();
        return result;
    }

    /**
     * 删除指定列簇
     * @param tableName
     * @param rowKey
     * @param falilyName
     */
    public static void deleteFalily(String tableName, String rowKey, String falilyName) {

        Table table = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            Delete delete =new Delete(Bytes.toBytes(rowKey));
            delete.deleteFamily(Bytes.toBytes(falilyName));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) {

        Table table = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            Delete delete =new Delete(Bytes.toBytes(rowKey));
            delete.deleteColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * 删除指定的某个rowkey
     * @param tableName
     * @param rowKey
     */
    public static void deleteByRowKey(String tableName, String rowKey){

        Table table = null;
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            Delete de =new Delete(Bytes.toBytes(rowKey));
            table.delete(de);
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * 让该表失效(只是失效)
     * @param tableName
     */
    public static void disableTable(String tableName){
        try {
            admin.disableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    public static void printResult(Result result){
        for(KeyValue kv: result.list()){
            System.out.println(Bytes.toString(kv.getFamily()));
            System.out.println(Bytes.toString(kv.getQualifier()));
            System.out.println(Bytes.toString(kv.getValue()));
            System.out.println(kv.getTimestamp());

        }
    }

    public static void close(){
        try {
            if(admin != null) {
                admin.close();
            }
            if(connection != null){
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static void setConfiguration(Configuration configuration) {
        HbaseUtils.configuration = configuration;
    }

    public static ExecutorService getExecutor() {
        return executor;
    }

    public static void setExecutor(ExecutorService executor) {
        HbaseUtils.executor = executor;
    }

    public static Admin getAdmin() {
        return admin;
    }

    public static void setAdmin(Admin admin) {
        HbaseUtils.admin = admin;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void setConnection(Connection connection) {
        HbaseUtils.connection = connection;
    }
}

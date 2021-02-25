package com.eco.data.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.util.Date;

/**
 * @auther lucifer
 * @mail wangxiyue.xy@163.com
 * @date 2021-02-24 13:24
 * @projectName hadoop-util
 * @description:
 */
public class HBaseUtil {

    private static Configuration configuration;
    private static Connection conn;
    private static Admin admin;

    private  static String TABLE_NAME = "surf_hbase_mul";

    public static void main(String[] args) {
        init();
        System.out.println("init ok");
        String[] col = new String[]{};
//        loadfile();
//        deleteTable(TABLE_NAME);
        Date st = new Date();
        String a  = getData(TABLE_NAME,"10","SURF_CHN_BASIC_INFO_ID","SURF_CHN_BASIC_INFO_ID");
        Date ed = new Date();
        System.out.println("val ===>" + a + " use time ===>" +(ed.getTime() - st.getTime()));
        close();
    }

    public static void loadfile(){
        try {
            String tableName = TABLE_NAME;
            File f = new File("d:/surf_chn_mul_full_min_20210112_202102221630.csv");
            BufferedReader br = null;
            br = new BufferedReader(new FileReader(f));
            String head = br.readLine();
            String[] headStr = head.split(",");
            String[] heads = new String[headStr.length];
            for (int i = 0; i < headStr.length; i++) {
                heads[i]=headStr[i].replaceAll("\"","");
            }

            String row  = br.readLine();
            String[] rows ;
            System.out.println("create table");
            createTable(TABLE_NAME,heads);

            Date st = new Date();
            while(row !=null){
                rows = row.split(",");
                if(rows.length==heads.length){
                    for (int i = 0; i < rows.length; i++) {
                        insertData(tableName,rows[0].replaceAll("\"",""),heads[i],heads[i],rows[i].replaceAll("\"",""));
                    }
                }
                row  =br.readLine();
            }
            Date ed = new Date();
            Long t = ed.getTime() - st.getTime();
            System.out.println("use time = " +t);

            System.out.println("end");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.198.192.70");
//        configuration.set("hbase.rootdir","hdfs://10.198.192.70:9000/hbase");


        try {
            conn = ConnectionFactory.createConnection(configuration);
            admin =conn.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String tableName,String[] col){
        TableName table = TableName.valueOf(tableName);
        try {
            if(admin.tableExists(table)){
                System.out.println(tableName + " 存在");
            }else{
                HTableDescriptor hTableDescriptor  = new HTableDescriptor(table);
                for (String colFamily : col) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insertData(String tableName,String rowKey,String colFamily,String col,String val){
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getData(String tableName,String rowKey,String colFamily,String col){
        try {
            Table table  = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addColumn(colFamily.getBytes(),col==null?null:col.getBytes());
            Result result = table.get(get);
            String val =  new String(result.getValue(colFamily.getBytes(),col==null?null:col.getBytes()));
            return val;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close(){
        try {
            if(admin!=null){
                admin.close();
            }
            if(conn!=null){
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("close");
    }

    public static void deleteTable(String tableName){
        try {
            TableName table = TableName.valueOf(tableName);
            if(admin== null || admin.isAborted() ){
                admin = conn.getAdmin();
            }
            admin.disableTable(table);
            admin.deleteTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("delete ===>"+tableName);
    }

}

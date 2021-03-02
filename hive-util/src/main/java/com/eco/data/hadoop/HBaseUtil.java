package com.eco.data.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HBaseUtil {

    private static Configuration configuration;
    private static Connection conn;
    private static Admin admin;

    private static String TABLE_NAME = "surf";
    private static String FILENAME  = "D:/资料/mut201806.csv";
    private static String TABLEFAMILAY  = "D:/资料/table.txt";
    private static String HEADFILE  = "D:/资料/head.txt";
    private static String IP = "10.198.192.76";

    private static int unit = 10000;

    public static void main(String[] args) {
        if(args !=null ){
            if(args.length == 4){
                FILENAME = args[0];
                HEADFILE = args[1];
                TABLE_NAME = args[2];
                IP = args[3];
            } else if(args.length != 0){

                System.out.println("java -jar FILENAME HEADFILE TABLE_NAME IP");
                return;
            }
        }
        System.out.println("FILENAME ==> " + FILENAME);
        System.out.println("HEADFILE ==> " + HEADFILE);
        System.out.println("TABLE_NAME ==> " + TABLE_NAME);
        System.out.println("IP ==> " + IP);

        init();
        System.out.println("init ok");
        String[] col = new String[]{};
        loadfile();
//        deleteTable(TABLE_NAME);
//        Date st = new Date();
//        String a  = getData(TABLE_NAME,"10","ID",null);
//        ResultScanner results = scanData(TABLE_NAME, "56778", null);
//        Date ed = new Date();
//        for (Result result : results) {
//            for (Cell c : result.listCells()) {
//                System.out.println(new String(c.getFamily()) + "==>" + new String(c.getQualifier()));
//            }
//            System.out.println("################");
//        }
//        System.out.println("use time ===>" +(ed.getTime() - st.getTime()));
        close();
    }

    public static void loadfile(){
        try {
            String tableName = TABLE_NAME;
            File f = new File(FILENAME);
            BufferedReader br = null;
            br = new BufferedReader(new FileReader(f));
            String[] heads = loadhead(HEADFILE);
            String[] familay = loadhead(TABLEFAMILAY);

            String row  = br.readLine();
            String[] rows ;
            System.out.println("create table");

            createTable(TABLE_NAME,familay);
            Date st = new Date();
            Table table = conn.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            long sum = 0;
            while(row !=null){
                rows = row.split(",");
                if(rows.length==heads.length){
                    String rowKey = rows[1].replaceAll("\"","") + "_" + rows[5].replaceAll("\"","");
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.setDurability(Durability.SKIP_WAL);

//                    put.addColumn(familay[0].getBytes(),familay[0].getBytes(),row.getBytes());
                    for (int i = 0; i < 20; i++) {
                        put.addColumn(familay[0].getBytes(),heads[i].getBytes(),rows[i].replaceAll("\"","").getBytes());
                    }
                    puts.add(put);
                    if(puts.size()>unit){//10w
                        Date subStart = new Date();
                        table.put(puts);
                        Date subEnd = new Date();
                        sum = sum + unit ;
                        System.out.println("sum ==> "+ sum +"条记录 "+ unit +" w  ==> " + (subEnd.getTime()-subStart.getTime()));
                        puts =new ArrayList<>();
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

    public static String[] loadhead(String headfile){
        try {
            File f = new File(headfile);
            BufferedReader br = null;
            br = new BufferedReader(new FileReader(f));
            String head = br.readLine();
            String[] headStr = head.split(",");
            String[] heads = new String[headStr.length];
            for (int i = 0; i < headStr.length; i++) {
                heads[i]=headStr[i].replaceAll("\"","");
            }
            return heads;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", IP);

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
                deleteTable(TABLE_NAME);
            }
            HTableDescriptor hTableDescriptor  = new HTableDescriptor(table);
            for (String colFamily : col) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
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

    public static ResultScanner scanData(String tableName,String rowKey,String colFamily){
        try {
            Table table  = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan(rowKey.getBytes());
            if(colFamily!=null){
                scan.addFamily(colFamily.getBytes());
            }


            ResultScanner resultScanner = table.getScanner(scan);
            return resultScanner;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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

package com.eco.data.hadoop;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @auther lucifer
 * @mail wangxiyue.xy@163.com
 * @date 2021-02-22 15:39
 * @projectName hadoop-util
 * @description:
 */
public class HiveUtil {

    static String jdbcDriver = "org.apache.hive.jdbc.HiveDriver";
    static String url = "jdbc:hive2://10.9.97.64:10000/surf";
    static String user = "root";
    static String password="12345";
    static Connection conn;
    static Statement stmt;

    public static void main(String[] args) {
        try {
            for (String arg : args) {
                System.out.println(arg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void init() throws  Exception{
        System.out.println("init");
        Class.forName(jdbcDriver);
        conn = DriverManager.getConnection(url,user,password);

        stmt = conn.createStatement();
        System.out.println("init ok");

        loadFile();
    }

    public static void loadFile() throws Exception {

        File f = new File("d:/surf_chn_mul_full_min_20210112_202102221630.csv");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String head = br.readLine();
        String[] heads = head.split(",");
        String row  = br.readLine();
        String[] rows;
        String sql = "CREATE TABLE `upar_wea_c_mul_min_new_202101` (\n" +
                "  `ID` bigint,\n" +
                "  `SURF_CHN_BASIC_INFO_ID` varchar(5) ,\n" +
                "  `Q_DATETIME` varchar(20) ,\n" +
                "  `D_DATETIME` varchar(20) ,\n" +
                "  `INSERT_TIME` varchar(20) ,\n" +
                "  `V04086` varchar(20),\n" +
                "  `V05015` varchar(20) ,\n" +
                "  `V06015` varchar(20) ,\n" +
                "  `V05001` varchar(20) ,\n" +
                "  `V06001` varchar(20) ,\n" +
                "  `V12001` varchar(20) ,\n" +
                "  `V07004` varchar(20),\n" +
                "  `V13003` varchar(20) ,\n" +
                "  `V11001` varchar(20) ,\n" +
                "  `V11002` varchar(20) ,\n" +
                "  `V10009` varchar(20) ,\n" +
                "  `filePath` varchar(255) \n" +
                ") ";

        System.out.println("create table");
        stmt.execute(sql);
        System.out.println("start");
        sql = "";
        while(row !=null){
            rows = row.split(",");
            if(rows.length==heads.length){
                String t = "";
                for (int i = 0; i < rows.length; i++) {
                    if(i==0){
                        t =t   +rows[i].replaceAll("\"","")+",";
                    }else {
                        t =t +  "'"+rows[i].replaceAll("\"","")+"',";
                    }

                }
                t= t.substring(0,t.length()-1);
                sql  =sql + "insert into table upar_wea_c_mul_min_new_202101 select " + t  + " ; ";
            }
            row  =br.readLine();
        }
        System.out.println("pre");
        stmt.execute(sql);
        System.out.println("end");
    }

//    public static void loadData() throws Exception {
//        String filePath = "/usr/local/hive/tmp/";
//        String sql = "load data local in path '" + filePath + "' overwrite into table t2";
//        System.out.println("Running: " + sql);
//        stmt.execute(sql);
//    }


}

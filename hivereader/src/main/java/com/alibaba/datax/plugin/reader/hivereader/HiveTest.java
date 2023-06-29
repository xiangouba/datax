package com.alibaba.datax.plugin.reader.hivereader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @program: DataX-master
 * @description:
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/9/9 14:48
 */
public class HiveTest {
    private static final Logger logger = LoggerFactory.getLogger(HiveTest.class);

    /**
     * hive驱动名称*
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    /**
     * 连接hive2服务的连接地址，Hive0.11.0以上版本提供了一个全新的服务：HiveServer2*
     */
//    private static String url = "jdbc:hive2://master:10000/default";
    private static String url = "jdbc:hive2://red-197.yingda.sa:21050/rawdata;auth=noSasl";
    /**
     * 对HDFS有操作权限的用户*
     */
    private static String user = "sa_cluster";
    /**
     * 在非安全模式下，指定一个用户运行查询，忽略密码*
     */
    private static String password = "";
    private static String sql = "";
    private static ResultSet res;

    public static void test() {
        try {

            //加载HiveServer2驱动程序
            logger.info("加载HiveServer2驱动程序");
            Class.forName(driverName);
            //根据URL连接指定的数据库
            logger.info("");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            //创建的表名
            String tableName = "testHiveDriverTable_0913";

//            //第一步:表存在就先删除
//            logger.info("第一步:表存在就先删除");
//            sql = "drop table " + tableName;
//            boolean del = stmt.execute(sql);
//            logger.info("删除结果：" + del);

//            //第二步:表不存在就创建
//            logger.info("第二步:表不存在就创建");
//            sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '\t' STORED AS TEXTFILE";
//            boolean cre = stmt.execute(sql);
//            logger.info("创建结果：" + cre);

            // 执行“show tables”操作
            logger.info("执行“show tables”操作");
            sql = "show tables '" + tableName + "'";
            res = stmt.executeQuery(sql);
            if (res.next()) {
                logger.info(res.getString(1));
            }

            // 执行“describe table”操作
            logger.info("执行“describe table”操作");
            sql = "describe " + tableName;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                logger.info(res.getString(1) + "\t" + res.getString(2));
            }

//            // 执行“load data into table”操作
//            logger.info("执行“load data into table”操作");
//            //hive服务所在节点的本地文件路径
//            String filepath = "/home/sa_cluster/testData/test4.txt";
//            sql = "load data local inpath '" + filepath + "' into table " + tableName;
//            boolean load = stmt.execute(sql);
//            logger.info("load结果：" + load);

            // 执行“select * query”操作
            logger.info("执行“select * query”操作");
            sql = "select * from " + tableName;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                logger.info(res.getInt(1) + "\t" + res.getString(2));
            }

            // 执行“regular hive query”操作，此查询会转换为MapReduce程序来处理
            logger.info("执行“regular hive query”操作，此查询会转换为MapReduce程序来处理");
            sql = "select count(*) from " + tableName;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                logger.info(res.getString(1));
            }

            conn.close();
            conn = null;
        } catch (Exception e) {
            logger.error("", e);
            System.exit(1);
        }
    }

    public static void test2() {
        try {

            //加载HiveServer2驱动程序
            logger.info("加载HiveServer2驱动程序");
            Class.forName(driverName);
            //根据URL连接指定的数据库
            logger.info("");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            // 执行“describe table”操作
            logger.info("执行“describe table”操作");
            sql = "describe events /*SA*/" ;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                logger.info(res.getString(1) + "\t" + res.getString(2));
            }

            // 执行“select * query”操作
            logger.info("执行“select * query”操作");
            sql = "select * from events /*SA*/";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                logger.info(res.getString(1) + "\t" + res.getDouble(2));
            }

            conn.close();
            conn = null;
        } catch (Exception e) {
            logger.error("", e);
            System.exit(1);
        }
    }



    public static void main(String[] args) {
        test2();
    }


}

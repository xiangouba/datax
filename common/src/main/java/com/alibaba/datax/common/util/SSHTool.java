package com.alibaba.datax.common.util;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @program: DataX-master
 * @description:
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/9/2 15:51
 */
public class SSHTool {

    private static final Logger logger = LoggerFactory.getLogger(SSHTool.class);

    private Connection conn;
    private String ipAddr;
    private int port;
    private Charset charset = StandardCharsets.UTF_8;
    private String userName;
    private String password;

    /**
     * *
     *
     * @param ipAddr   IP
     * @param userName 用户名
     * @param password 密码
     * @param charset  编码
     */
    public SSHTool(String ipAddr, int port, String userName, String password, Charset charset) {
        this.ipAddr = ipAddr;
        this.port = port;
        this.userName = userName;
        this.password = password;
        if (charset != null) {
            this.charset = charset;
        }
    }

    /**
     * 登录远程Linux主机
     *
     * @return 是否登录成功
     */
    private boolean login() {
        conn = new Connection(ipAddr, port);
        try {
            // 连接
            conn.connect();
            // 认证
            return conn.authenticateWithPassword(userName, password);
        } catch (Exception e) {
            logger.error("登录异常：" + ipAddr + "   " + userName, e);
            return false;
        }
    }


    /**
     * 执行Shell脚本或命令
     *
     * @param cmds 命令行序列
     * @return 脚本输出结果
     */
    public StringBuilder exec(String cmds) throws Exception {
        Session session = null;
        InputStream in = null;
        StringBuilder result = new StringBuilder();
        try {
            if (this.login()) {
                // 打开一个会话
                session = conn.openSession();
                session.execCommand(cmds);
                in = session.getStdout();
                result = this.processStdout(in, this.charset);

                logger.info("命令执行结果：" + session.getStdout() + " " + session.getStderr() + "  " + session.getExitStatus() + "   " + session.getExitSignal());
                logger.info("命令执行结果： \nStdout：" + this.processStdout(session.getStdout(), this.charset).toString() + " \nStderr：" + this.processStdout(session.getStderr(), this.charset).toString() + "  \nStatus：" + session.getExitStatus() + "   \nSignal：" + session.getExitSignal());
            }
        } finally {
            if (null != in) {
                in.close();
            }
            if (null != session) {
                session.close();
            }
            if (null != conn) {
                conn.close();
            }
        }
        return result;
    }

    /**
     * 解析流获取字符串信息
     *
     * @param in      输入流对象
     * @param charset 字符集
     * @return 脚本输出结果
     */
    public StringBuilder processStdout(InputStream in, Charset charset) throws Exception {
        byte[] buf = new byte[1024];
        StringBuilder sb = new StringBuilder();
        try {
            // 此方法是休息10秒后最后一次性输出2行数据
            int length;
            while ((length = in.read(buf)) != -1) {
                sb.append(new String(buf, 0, length));
            }

            // 这个会按照脚本一步一步执行，中途有休息10秒。
            BufferedReader reader = null;
            String result = null;
            //reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            reader = new BufferedReader(new InputStreamReader(in, charset.name()));
            while ((result = reader.readLine()) != null) {
                System.out.println(result);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
        return sb;
    }

    // 脚本执行结束返回
    public static void main(String[] args) {
        try {

            SSHTool tool = new SSHTool("10.1.10.197", 20022, "root", "Linux&test2018", StandardCharsets.UTF_8);
//            System.out.println("------exec: " + tool.exec("hive -e \":; use rowdata; create table tmp_datax_hivereader_20220902_1662083347851182741 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\u0001' STORED AS TEXTFILE  as SELECT user_id,distinct_id,event,time,$lib as lib  FROM rawdata.events WHERE `date` = CURRENT_DATE() LIMIT 10 ;\" "));
//            System.out.println("------exec: " + tool.exec("hive -e \":; use rowdata; create table tmp_datax_hivereader_20220902_1662083347851182741 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\u0001' STORED AS TEXTFILE  as SELECT user_id,distinct_id,event,time,$lib as lib  FROM rawdata.events WHERE `date` = CURRENT_DATE() LIMIT 10 ;\" | grep -v \"WARN\""));
            System.out.println("------exec: " + tool.exec("hive -e \"  use rawdata; create table tmp_datax_hivereader_20220902_1662083347851182741 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS TEXTFILE  as SELECT user_id,distinct_id,event,time,$lib as lib  FROM rawdata.events WHERE `date` = CURRENT_DATE() LIMIT 10  ;\" | grep -v \"WARN\" "));

        } catch (Exception e) {
            logger.error("", e);
        }
    }
}

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Gbase8aTest {

    private static final Logger logger = LoggerFactory.getLogger(Gbase8aTest.class);


    private static String driverName = "com.gbase.jdbc.Driver";
//    private static String url = "jdbc:gbase://10.1.11.188:15258/test?failoverEnable=true&gclusterId=bmsoft&useUnicode=true&characterEncoding=UTF-8";
    private static String url = "jdbc:gbase://10.1.11.188:15258/test?failoverEnable=true&gclusterId=bmsoft&useUnicode=true";
    private static String user = "gbase";
    private static String password = "gbase20110531";
    private static String sql = "";
    private static ResultSet res;

    public static void test() {
        try {

            logger.info("加载驱动程序");
            Class.forName(driverName);
            //根据URL连接指定的数据库
            Connection conn = DriverManager.getConnection(url, user, password);

            Statement stmt = conn.createStatement();

            Statement stmt2 = conn.createStatement();
            boolean b = stmt2.execute("set names utf8mb4");
            logger.info("" + b);

            sql = "INSERT INTO test.yhxw_events (name) VALUES('min❤\uD83C\uDE36\uD83D\uDD12\uD83D\uDC2D')";
            logger.info("执行sql：" + sql);
            boolean b1 = stmt.execute(sql);
            logger.info("" + b1);

            conn.close();
            conn = null;
        } catch (Exception e) {
            logger.error("", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        test();
    }
}

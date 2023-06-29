package com.alibaba.datax.plugin.reader.hivereader;

/**
 * @author dean 2019/10/25.
 * @version v1.1
 */
public class Key {

    /**
     * 1.必选:jdbcUrl、userName、hiveSql
     * 2.可选(有缺省值):
     * 			driverName(org.apache.hive.jdbc.HiveDriver)
     * 3.可选(无缺省值):password
     * */

    /**
     *  hive驱动名称
     */
    public final static String DRIVER_NAME = "driverName";
    /**
     * 连接hive2服务的连接地址，Hive0.11.0以上版本提供了一个全新的服务：HiveServer2
     */
    public final static String JDBC_URL = "jdbcUrl";
    /**
     * 对HDFS有操作权限的用户
     */
    public final static String USER_NAME = "userName";
    /**
     * 在非安全模式下，指定一个用户运行查询，忽略密码
     */
    public final static String PASSWORD = "password";
    /**
     * reader执行的hiveSql语句
     */
    public final static String HIVE_SQL = "hiveSql";

    public final static String MANDATORY_ENCODING = "mandatoryEncoding";

    /**
     * SQL超时时间（秒）
     */
    public final static String QUERY_TIME_OUT = "queryTimeout";

}

package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dean 2019/10/25.
 * @version v1.1
 */
public class HiveReader extends Reader {


    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */

    public static class Job extends Reader.Job {

        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration readerOriginConfig = null;


        @Override
        public void init() {
            logger.info("init() begin...");
            //获取配置文件信息{parameter 里面的参数}
            this.readerOriginConfig = super.getPluginJobConf();
            //检查配置
            this.validate();
            logger.info("init() ok and end...");
        }

        /**
         * 检查配置
         */
        private void validate() {
            //是否配置 jdbcUrl、userName、hiveSql
            this.readerOriginConfig.getNecessaryValue(Key.JDBC_URL, HiveReaderErrorCode.JDBC_URL_NOT_FIND_ERROR);
            this.readerOriginConfig.getNecessaryValue(Key.USER_NAME, HiveReaderErrorCode.USER_NAME_NOT_FIND_ERROR);
            this.readerOriginConfig.getNecessaryList(Key.HIVE_SQL, HiveReaderErrorCode.SQL_NOT_FIND_ERROR);
        }


        @Override
        public List<Configuration> split(int adviceNumber) {
            //按照Hive  sql的个数 获取配置文件的个数
            logger.info("split() begin...");
            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedConfig = null;
            for (String querySql : sqls) {
                splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Key.HIVE_SQL, querySql);
                readerSplitConfigs.add(splitedConfig);
            }
            return readerSplitConfigs;
        }

        //全局post
        @Override
        public void post() {
            logger.info("任务执行完毕,hive reader post");

        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;

        private String driverName;
        private String jdbcUrl;
        private String userName;
        private String password;
        private String hiveSql;
        private String mandatoryEncoding;
        private int queryTimeout;

        Connection conn;
        Statement stmt;
        ResultSet rs;
        int columnNumber = 0;

        private static final boolean IS_DEBUG = logger.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        //默认超时时间（秒）
        private static final int DEFAULT_QUERY_TIME_OUT = 3;
        //最大尝试次数
        private static final int MAX_QUERY_TRY_NUM = 3;
        //尝试间隔时间（秒）
        private static final int QUERY_SLEEP_TIME = 10;

        @Override
        public void init() {
            //获取配置
            this.taskConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件

            //获取 driverName、jdbcUrl、userName、hiveSql,QueryTimeout
            this.driverName = taskConfig.getString(Key.DRIVER_NAME, Constant.DRIVER_NAME);
            this.jdbcUrl = taskConfig.getString(Key.JDBC_URL);
            this.userName = taskConfig.getString(Key.USER_NAME);
            this.hiveSql = taskConfig.getString(Key.HIVE_SQL);
            this.mandatoryEncoding = taskConfig.getString(Key.MANDATORY_ENCODING, "");
            this.queryTimeout = taskConfig.getInt(Key.QUERY_TIME_OUT, DEFAULT_QUERY_TIME_OUT);

            //判断sql语句的结尾是否是分号，不是给加一个
            if (!this.hiveSql.trim().endsWith(Constant.END_CHARACTER)) {
                this.hiveSql = this.hiveSql + Constant.END_CHARACTER;
            }

            connection();
        }

        @Override
        public void startRead(RecordSender recordSender) {

            logger.info("read start");

            try {
                logger.info("执行sql：" + hiveSql);
                //执行sql
//                rs = stmt.executeQuery(hiveSql);
                executeQuery(0);

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                while (rs.next()) {
                    transportOneRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, super.getTaskPluginCollector());
                }

            } catch (Exception e) {
                logger.error("hive读取异常：", e);
            }

            logger.info("end read source files...");
        }

        @Override
        public void destroy() {
            logger.info("hive read destroy...");

            try {
                if (null != rs || !rs.isClosed()) {
                    rs.close();
                }
            } catch (Exception e) {
                logger.error("关闭 ResultSet 异常：", e);
            }
            try {
                if (!stmt.isClosed()) {
                    stmt.close();
                }
            } catch (Exception e) {
                logger.error("关闭 Statement 异常：", e);
            }
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.error("关闭 Connection 异常：", e);
            }

        }


        public void executeQuery(int tryNum) {
            try {
                if (tryNum > MAX_QUERY_TRY_NUM) {
                    logger.info(String.format("大于最大尝试次数 %S ,获取数据失败！", MAX_QUERY_TRY_NUM));
                    throw new RuntimeException("获取数据失败!");
                }

                if (tryNum != 0) {
                    logger.info(String.format("获取数据失败，将在 %s 秒后开始第 %s 次尝试！", QUERY_SLEEP_TIME, tryNum));
                    try {
                        Thread.sleep(QUERY_SLEEP_TIME * 1000);
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
                rs = stmt.executeQuery(hiveSql);
            } catch (Exception e) {

                logger.error("执行sql语句异常：", e);

                if (e.getMessage().contains("Read timed out")) {
                    executeQuery(tryNum + 1);
                } else {
                    throw new RuntimeException("获取数据异常");
                }
            }
        }

        public void connection() {
            try {
                //加载HiveServer2驱动程序
                logger.info("加载HiveServer2驱动程序");
                Class.forName(driverName);
                //根据URL连接指定的数据库
                logger.info("根据URL连接指定的数据库");
                conn = DriverManager.getConnection(jdbcUrl, userName, password);
                //创建用于向数据库发送SQL语句的语句对象
                stmt = conn.createStatement();
                //超时时间
                logger.info("设置超时时间(秒)：" + queryTimeout);
//                stmt.setQueryTimeout(queryTimeout * 1000);
                stmt.setQueryTimeout(queryTimeout);

            } catch (Exception e) {
                logger.error("hive数据库连接异常:", e);
                throw DataXException.asDataXException(HiveReaderErrorCode.CONNECTION_ERROR, "hive数据库连接异常!");
            }

        }


        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                            TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException.asDataXException(HiveReaderErrorCode.UNSUPPORTED_TYPE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                            metaData.getColumnName(i),
                                            metaData.getColumnType(i),
                                            metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    logger.debug("read data " + record.toString() + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

        public void printColumnName(ResultSetMetaData metaData) {

            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= columnNumber; i++) {
                try {
                    sb.append(metaData.getColumnName(i));
                } catch (Exception e) {
                    logger.error("", e);
                }

                if (i < columnNumber) {
                    sb.append(",");
                }
            }

            logger.error("数据库字段名称：" + sb.toString());
        }
    }

}

package com.alibaba.datax.plugin.writer.mysqloraclewriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil.doCheckBatchSize;

/**
 * @Author gxx
 * @Date 2023年06月08日11时19分
 */
public class OriginalConfUtil {

    private static final Logger logger = LoggerFactory.getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType MYSQLDATABASE;
    public static DataBaseType ORACLEDATABASE;


    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType1,DataBaseType dataBaseType2) {
        // 检查 MySQL的 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME_MYSQL, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD_MYSQL, DBUtilErrorCode.REQUIRED_VALUE);
        //检查 Oracle的 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME_ORACLE, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD_ORACLE, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        dealColumnConfMysql(originalConfig);
        dealColumnConfOracle(originalConfig);

        dealWriteMode(originalConfig, dataBaseType1,dataBaseType2);
    }

    /**
     * 校验配置填写的数据库连接信息
     * 设置配置对象 Constant.TABLE_NUMBER_MARK = "tableNumber" 属性 tableNum
     * @param originalConfig  配置json
     */
    public static void simplifyConf(Configuration originalConfig) {

        List<Object> connectionMysql = originalConfig.getList(Constant.CONNECTION_MYSQL, Object.class);

        List<Object> connectionOracle = originalConfig.getList(Constant.CONNECTION_ORACLE, Object.class);

        int tableNum = 0;

        //mysql
        for (int i = 0, len = connectionMysql.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connectionMysql.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = MYSQLDATABASE.appendJDBCSuffixForWriter(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONNECTION_MYSQL, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(MYSQLDATABASE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONNECTION_MYSQL, i, Key.TABLE), expandedTables);
        }

        //oracle
        for (int i = 0, len = connectionOracle.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connectionOracle.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = ORACLEDATABASE.appendJDBCSuffixForWriter(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONNECTION_ORACLE, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(ORACLEDATABASE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONNECTION_ORACLE, i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    /**
     * MySQL 数据库连接检查设定
     * @param originalConfig
     */
    public static void dealColumnConfMysql(Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONNECTION_MYSQL, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME_MYSQL);
        String password = originalConfig.getString(Key.PASSWORD_MYSQL);
        String oneTable = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONNECTION_MYSQL, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(MYSQLDATABASE, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    /**
     * Oracle数据库连接检查设定
     * @param originalConfig
     */
    public static void dealColumnConfOracle(Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONNECTION_ORACLE, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME_ORACLE);
        String password = originalConfig.getString(Key.PASSWORD_ORACLE);
        String oneTable = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONNECTION_ORACLE, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(ORACLEDATABASE, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
            List<String> allColumns;
            if (isPreCheck){
                allColumns = DBUtil.getTableColumnsByConn(MYSQLDATABASE,connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
            }else{
                allColumns = DBUtil.getTableColumnsByConn(MYSQLDATABASE,connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
            }

            logger.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                logger.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (userConfiguredColumns.size() > allColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
            } else {

                //本插件两个表的字段不同 所以限定 column 只能填写 *
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您配置的写入数据库表的列为必须 * . 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
//                // 确保用户配置的 column 不重复
//                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);
//
//                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
//                DBUtil.getColumnMetaData(connectionFactory.getConnecttion(), oneTable,StringUtils.join(userConfiguredColumns, ","));
            }
        }
    }

    /**
     * 预编写 执行的sql
     *  insert INTO %s ( allColumns )values(  ?,?,?,?,?,?,?,?,.....)
     * @param originalConfig
     * @param dataBaseType1 mysql
     * @param dataBaseType2 oracle
     */
    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType1,DataBaseType dataBaseType2) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrlMysql = originalConfig.getString(String.format("%s[0].%s", Constant.CONNECTION_MYSQL, Key.JDBC_URL, String.class));
        String jdbcUrlOracle0 = originalConfig.getString(String.format("%s[0].%s", Constant.CONNECTION_ORACLE, Key.JDBC_URL, String.class));
        String jdbcUrlOracle1 = originalConfig.getString(String.format("%s[1].%s", Constant.CONNECTION_ORACLE, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        boolean forceUseUpdate = false;

        //mysql
        String writeDataMysqlTemplate = getWriteTemplate(columns, valueHolders, writeMode,dataBaseType1, forceUseUpdate,true);
        //oracle 参数是 0
        String writeDataOracleTemplate0 = getWriteTemplate(columns, valueHolders, writeMode,dataBaseType2, forceUseUpdate,true);
        //oracle 参数是 1
        String writeDataOracleTemplate1 = getWriteTemplate(columns, valueHolders, writeMode,dataBaseType2, forceUseUpdate,false);

        logger.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataMysqlTemplate, jdbcUrlMysql);
        logger.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataOracleTemplate0, jdbcUrlOracle0);
        logger.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataOracleTemplate1, jdbcUrlOracle1);

        originalConfig.set(Constant.INSERT_MYSQL_TEMPLATE_MARK, writeDataMysqlTemplate);
        originalConfig.set(Constant.INSERT_ORACLE_TEMPLATE_MARK_0, writeDataOracleTemplate0);
        originalConfig.set(Constant.INSERT_ORACLE_TEMPLATE_MARK_1, writeDataOracleTemplate1);
    }


    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate,Boolean flag) {
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("replace")
                || writeMode.trim().toLowerCase().startsWith("update");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        // && writeMode.trim().toLowerCase().startsWith("replace")
        String writeDataSqlTemplate;
        if (forceUseUpdate ||
                ((dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Tddl) && writeMode.trim().toLowerCase().startsWith("update"))
        ) {
            //不做
            writeDataSqlTemplate = null;
        } else {

            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (writeMode.trim().toLowerCase().startsWith("update")) {
                writeMode = "replace";
            }

            if (DataBaseType.MySql.equals(dataBaseType)){
                writeDataSqlTemplate = new StringBuilder().append(writeMode)
                        .append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
                        .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                        .append(")").toString();
            }else {
                //参数0
                if (flag){
                    writeDataSqlTemplate = new StringBuilder().append(writeMode)
                            .append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
                            .append(",issuccess")
                            .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                            .append(",'0')").toString();
                }else {
                    //参数 1
                    writeDataSqlTemplate = new StringBuilder().append(writeMode)
                            .append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
                            .append(",issuccess")
                            .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                            .append(",'1')").toString();
                }
            }
        }

        return writeDataSqlTemplate;
    }
}

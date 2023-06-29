package com.alibaba.datax.plugin.writer.mysqloraclewriter.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author gxx
 * @Date 2023年06月08日11时08分
 */
public class CommonUtil {
    public static class Job{
        private DataBaseType mysqlDataBase;
        private DataBaseType oracleDataBase;
        private static final Logger logger = LoggerFactory.getLogger(CommonUtil.Job.class);

        public Job(DataBaseType dataBaseType1,DataBaseType dataBaseType2) {
            this.mysqlDataBase = dataBaseType1;
            this.oracleDataBase = dataBaseType2;
            OriginalConfUtil.MYSQLDATABASE = this.mysqlDataBase;
            OriginalConfUtil.ORACLEDATABASE = this.oracleDataBase;
        }

        public void init(Configuration originalConfig) {
            OriginalConfUtil.doPretreatment(originalConfig, this.mysqlDataBase,this.oracleDataBase);
            logger.debug("After job init(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }
    }

    public static class Task{
        protected static final Logger logger = LoggerFactory.getLogger(CommonRdbmsWriter.Task.class);

        private DataBaseType mysqlDataBase;
        private DataBaseType oracleDataBase;
        private static final String VALUE_HOLDER = "?";

        protected String usernameMysql;
        protected String usernameOracle;
        protected String passwordMysql;
        protected String passwordOracle;
        protected String jdbcUrlMysql;
        protected String jdbcUrlOracle;
        protected String table;
        protected List<String> columns;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected Boolean deleteFlag;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;

        protected static String INSERT_MYSQL_TEMPLATE;
        protected static String INSERT_ORACLE0_TEMPLATE;
        protected static String INSERT_ORACLE1_TEMPLATE;

        protected String mysqlRecordSql;
        protected String oracle0RecordSql;
        protected String oracle1RecordSql;

        protected String deleteRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        public Task(DataBaseType dataBaseType1,DataBaseType dataBaseType2) {
            this.mysqlDataBase = dataBaseType1;
            this.oracleDataBase = dataBaseType2;
            OriginalConfUtil.MYSQLDATABASE = this.mysqlDataBase;
            OriginalConfUtil.ORACLEDATABASE = this.oracleDataBase;
        }

        public void init(Configuration writerSliceConfig){
            this.usernameMysql = writerSliceConfig.getString(Key.USERNAME_MYSQL);
            this.passwordMysql = writerSliceConfig.getString(Key.PASSWORD_MYSQL);
            this.usernameOracle = writerSliceConfig.getString(Key.USERNAME_ORACLE);
            this.passwordOracle = writerSliceConfig.getString(Key.PASSWORD_ORACLE);
            //TODO jdbc 待确定
            this.jdbcUrlMysql = writerSliceConfig.getString(Key.JDBC_URL);
            this.jdbcUrlOracle = writerSliceConfig.getString(Key.JDBC_URL);

            //mysql 处理逻辑
            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            this.deleteFlag = (null != writerSliceConfig.getBool(Key.DELETE_FLAG)) ? writerSliceConfig.getBool(Key.DELETE_FLAG) : false;
            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            INSERT_MYSQL_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_MYSQL_TEMPLATE_MARK);
            this.mysqlRecordSql = String.format(INSERT_MYSQL_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    jdbcUrlMysql, this.table);

            //oracle 处理逻辑

            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            this.deleteFlag = (null != writerSliceConfig.getBool(Key.DELETE_FLAG)) ? writerSliceConfig.getBool(Key.DELETE_FLAG) : false;
            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            INSERT_ORACLE0_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_ORACLE_TEMPLATE_MARK_0);
            this.oracle0RecordSql = String.format(INSERT_ORACLE0_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    jdbcUrlOracle, this.table);
        }

        public void startWrite(RecordReceiver recordReceiver,
                               Configuration writerSliceConfig,
                               TaskPluginCollector taskPluginCollector){
            //mysql 连接信息
            Connection connectionMysql = DBUtil.getConnection(this.mysqlDataBase, this.jdbcUrlMysql, usernameMysql, passwordMysql);
            //oracle 连接信息
            Connection connectionOracle = DBUtil.getConnection(this.oracleDataBase, this.jdbcUrlOracle, usernameOracle, passwordOracle);

//            DBUtil.dealWithSessionConfig(connectionMysql, writerSliceConfig, this.mysqlDataBase, BASIC_MESSAGE);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connectionMysql);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connectionOracle);
        }


        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection){
            this.taskPluginCollector = taskPluginCollector;
            // 写数据库的SQL语句
            calcWriteRecordSql();
            calcWriteRecordSql1();
            calcWriteRecordSql2();

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer,this.mysqlRecordSql,mysqlDataBase,true);
                        doBatchInsert(connection, writeBuffer,this.oracle0RecordSql,oracleDataBase,false);
                        doBatchInsert(connection, writeBuffer,this.oracle1RecordSql,oracleDataBase,true);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer,this.mysqlRecordSql,mysqlDataBase,true);
                    doBatchInsert(connection, writeBuffer,this.oracle0RecordSql,oracleDataBase,false);
                    doBatchInsert(connection, writeBuffer,this.oracle1RecordSql,oracleDataBase,true);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, connection);
            }
        }
        protected void doBatchInsert(Connection connection, List<Record> buffer,String sql,DataBaseType dataBaseType,Boolean flag)
                throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(sql);

                for (Record record : buffer) {
                    if (!dataBaseType.equals(DataBaseType.MySql)){
                        //TODO 确定 状态字段的位置
                        if (record.getColumn(0).asString() == "1" && flag){
                            break;
                        }
                    }
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record,dataBaseType);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                logger.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer,sql,dataBaseType);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record,DataBaseType dataBaseType)
                throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                String typeName = this.resultSetMetaData.getRight().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, typeName, record.getColumn(i),dataBaseType);
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, String typeName, Column column,DataBaseType dataBaseType) throws SQLException {
            java.util.Date utilDate;

            if (null == column) {
                preparedStatement.setNull(columnIndex + 1, columnSqltype);
                return preparedStatement;
            }

            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, longValue.toString());
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (typeName == null) {
                        typeName = this.resultSetMetaData.getRight().get(columnIndex);
                    }

                    if (typeName.equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        } catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                    break;

                case Types.BOOLEAN:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    } else {
                        preparedStatement.setString(columnIndex + 1, column.asString());
                    }
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.UNSUPPORTED_TYPE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                            this.resultSetMetaData.getLeft()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getMiddle()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getRight()
                                                    .get(columnIndex)));
            }
            return preparedStatement;
        }

        protected void doOneInsert(Connection connection, List<Record> buffer,String sql,DataBaseType dataBaseType) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection
                        .prepareStatement(sql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record,dataBaseType);
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        logger.debug(e.toString());

                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    } finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }
        private void calcWriteRecordSql() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                boolean forceUseUpdate = false;

                INSERT_MYSQL_TEMPLATE = OriginalConfUtil.getWriteTemplate(columns, valueHolders, writeMode, mysqlDataBase, forceUseUpdate,true);
                mysqlRecordSql = String.format(INSERT_MYSQL_TEMPLATE, this.table);
            }
        }
        private void calcWriteRecordSql1() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                boolean forceUseUpdate = false;

                INSERT_MYSQL_TEMPLATE = OriginalConfUtil.getWriteTemplate(columns, valueHolders, writeMode, mysqlDataBase, forceUseUpdate,true);
                oracle0RecordSql = String.format(INSERT_MYSQL_TEMPLATE, this.table);
            }
        }
        private void calcWriteRecordSql2() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                boolean forceUseUpdate = false;

                INSERT_MYSQL_TEMPLATE = OriginalConfUtil.getWriteTemplate(columns, valueHolders, writeMode, mysqlDataBase, forceUseUpdate,false);
                oracle1RecordSql = String.format(INSERT_MYSQL_TEMPLATE, this.table);
            }
        }

        protected String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }
    }
}

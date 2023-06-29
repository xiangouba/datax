package com.alibaba.datax.plugin.writer.shanghaiqibuwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.writer.shanghaiqibuwriter.writer.Key;
import com.alibaba.datax.plugin.writer.shanghaiqibuwriter.writer.util.DBUtilNew;
import com.alibaba.datax.plugin.writer.shanghaiqibuwriter.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.writer.shanghaiqibuwriter.writer.util.WriterUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;


/**
 * @program: DataX-master
 * @description:
 * @author: gxx
 * @version: 1.0
 * @Create: 2023/3/27 10:02
 */
public class ShanghaiQiBuWriter extends Writer {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Gbase;

    public static class Job extends Writer.Job {
        private DataBaseType dataBaseType;


        private static final Logger logger = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void preCheck() {
            this.init();
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.dataBaseType = DATABASE_TYPE;
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            logger.debug("After job init(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        @Override
        public void prepare() {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(com.alibaba.datax.plugin.rdbms.writer.Key.USERNAME);
                String password = originalConfig.getString(com.alibaba.datax.plugin.rdbms.writer.Key.PASSWORD);

                List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                        Object.class);
                Configuration connConf = Configuration.from(conns.get(0)
                        .toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL);
                originalConfig.set(com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE, String.class).get(0);
                originalConfig.set(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE, table);

                List<String> preSqls = originalConfig.getList(com.alibaba.datax.plugin.rdbms.writer.Key.PRE_SQL, String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);

                originalConfig.remove(Constant.CONN_MARK);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    originalConfig.remove(com.alibaba.datax.plugin.rdbms.writer.Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                    logger.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            logger.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);

            //处理单表的情况
            if (tableNumber == 1) {
                //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
                for (int j = 0; j < adviceNumber; j++) {
                    splitResultConfigs.add(originalConfig.clone());
                }

                return splitResultConfigs;
            }

            if (tableNumber != adviceNumber) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                                tableNumber, adviceNumber));
            }

            String jdbcUrl;
            List<String> preSqls = originalConfig.getList(com.alibaba.datax.plugin.rdbms.writer.Key.PRE_SQL, String.class);
            List<String> postSqls = originalConfig.getList(com.alibaba.datax.plugin.rdbms.writer.Key.POST_SQL, String.class);

            List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);

            for (Object conn : conns) {
                Configuration sliceConfig = originalConfig.clone();

                Configuration connConf = Configuration.from(conn.toString());
                jdbcUrl = connConf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL);
                sliceConfig.set(com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL, jdbcUrl);

                sliceConfig.remove(Constant.CONN_MARK);

                List<String> tables = connConf.getList(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE, String.class);

                for (String table : tables) {
                    Configuration tempSlice = sliceConfig.clone();
                    tempSlice.set(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE, table);
                    tempSlice.set(com.alibaba.datax.plugin.rdbms.writer.Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                    tempSlice.set(com.alibaba.datax.plugin.rdbms.writer.Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                    splitResultConfigs.add(tempSlice);
                }

            }

            return splitResultConfigs;
        }

        public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
            if (null == preOrPostSqls) {
                return Collections.emptyList();
            }

            List<String> renderedSqls = new ArrayList<String>();
            for (String sql : preOrPostSqls) {
                //preSql为空时，不加入执行队列
                if (StringUtils.isNotBlank(sql)) {
                    renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
                }
            }

            return renderedSqls;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {
        protected static final Logger logger = LoggerFactory.getLogger(CommonRdbmsWriter.Task.class);

        protected DataBaseType dataBaseType;
        private static final String VALUE_HOLDER = "?";

        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected String primaryKey;
        protected List<String> columns;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;
        protected List<String> preSqls;
        protected List<String> postSqls;
        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;

        protected static String INSERT_OR_REPLACE_TEMPLATE;
        protected static String DELETE_TEMPLATE;

        protected String writeRecordSql;
        protected String deleteSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        private Configuration writerSliceConfig;
        private ShanghaiQiBuWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.dataBaseType = DATABASE_TYPE;
            writerSliceConfig = super.getPluginJobConf();
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);

            logger.info("--------------- write init : " + username + "  " + password + "  " + jdbcUrl + "   " + dataBaseType);


            StringBuffer a = new StringBuffer();
            //ob10的处理
            if (this.jdbcUrl.startsWith(Constant.OB10_SPLIT_STRING) && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                logger.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() + ":" + this.username;
                this.jdbcUrl = ss[2];
                logger.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
//            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
//            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        @Override
        public void prepare() {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);

            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                    this.dataBaseType, BASIC_MESSAGE);

            int tableNumber = writerSliceConfig.getInt(
                    Constant.TABLE_NUMBER_MARK);
            if (tableNumber != 1) {
                logger.info("Begin to execute preSqls:[{}]. context info:{}.",
                        StringUtils.join(this.preSqls, ";"), BASIC_MESSAGE);
                WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE, dataBaseType);
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver, RecordSender recordSender) {
            this.writerSliceConfig = super.getPluginJobConf();
            startWriteNew(lineReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }

        public void startWriteNew(RecordReceiver recordReceiver,
                                  Configuration writerSliceConfig,
                                  TaskPluginCollector taskPluginCollector) {
            Connection connection = DBUtilNew.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
            DBUtilNew.dealWithSessionConfig(connection, writerSliceConfig,
                    this.dataBaseType, BASIC_MESSAGE);
            this.primaryKey = (String) writerSliceConfig.getList(Key.PRIMARY_KEY).get(0);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
        }

        //具体的写入方法
        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection) {
            this.taskPluginCollector = taskPluginCollector;
            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtilNew.getColumnMetaData(connection,
                    this.table, StringUtils.join(this.columns, ","));
            // 写数据库的SQL语句
            calcWriteRecordSql();
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            List<Record> deleteBuffer = new ArrayList<>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    bufferBytes += record.getMemorySize();
                    if ("D".equals(record.getColumn(1).getRawData()) || "U".equals(record.getColumn(1).getRawData())) {
                        deleteBuffer.add(record);
                    }
                    if ("I".equals(record.getColumn(1).getRawData()) || "U".equals(record.getColumn(1).getRawData())) {
                        writeBuffer.add(record);
                    }
                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize || deleteBuffer.size() >= batchSize) {
                        if (deleteBuffer != null && !deleteBuffer.isEmpty()) {
                            delete(connection, deleteBuffer);
                        }
                        if (writeBuffer != null && !writeBuffer.isEmpty()) {
                            doBatchInsert(connection, writeBuffer);
                        }
                        writeBuffer.clear();
                        deleteBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (deleteBuffer != null && !deleteBuffer.isEmpty()) {
                    delete(connection, deleteBuffer);
                    deleteBuffer.clear();
                }
                if (writeBuffer != null && !writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                }
                bufferBytes = 0;

            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                deleteBuffer.clear();
                bufferBytes = 0;
                DBUtilNew.closeDBResources(null, null, connection);
            }
        }

        private void calcWriteRecordSql() {
//            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
            List<String> valueHolders = new ArrayList<String>(columnNumber);
            for (int i = 0; i < columns.size(); i++) {
                String type = resultSetMetaData.getRight().get(i);
                valueHolders.add(calcValueHolder(type));
            }

            boolean forceUseUpdate = false;
            //ob10的处理
            if (dataBaseType != null && dataBaseType == DataBaseType.MySql && OriginalConfPretreatmentUtil.isOB10(jdbcUrl)) {
                forceUseUpdate = true;
            }

            //sql模板
            INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, forceUseUpdate);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
//            }
        }


        /**
         * 删除sql
         */
        protected void calcDeleteSql() {
            //sql模板
            DELETE_TEMPLATE = WriterUtil.getDeleteTemplate();
            this.deleteSql = String.format(DELETE_TEMPLATE, this.table, this.primaryKey);
        }

        protected synchronized String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }

        protected void doBatchInsert(Connection connection, List<Record> buffer)
                throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                logger.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtilNew.closeDBResources(preparedStatement, null);
            }
        }

        protected void delete(Connection connection, List<Record> buffer)
                throws SQLException {
            PreparedStatement preparedStatement = null;
            //删除的数据
            List<Map<String, Object>> deleteList = new ArrayList<>();
            for (Record record : buffer) {
                //删除的数据
                deleteList.add(JSONObject.parseObject(record.getColumn(6).getRawData().toString().toLowerCase(Locale.ROOT)));
            }
            //删除的参数
            StringBuilder paramSql = new StringBuilder();
//            for (Record record : buffer) {
//                paramSql.append(record.getColumn(0).getRawData()).append("','");
//            }
            for (Map<String, Object> objectMap : deleteList) {
                paramSql.append(objectMap.get(this.primaryKey.toLowerCase(Locale.ROOT))).append("','");
            }
            //删除拼接后多余的最后的两个字符
            paramSql.deleteCharAt(paramSql.length() - 1);
            paramSql.deleteCharAt(paramSql.length() - 1);
            paramSql.append(")");
            try {
                connection.setAutoCommit(false);
                //删除的SQL语句
                calcDeleteSql();
                this.deleteSql = this.deleteSql + paramSql;
                //DELETE FROM %s WHERE %s in ('
                preparedStatement = connection
                        .prepareStatement(this.deleteSql);
                preparedStatement.executeUpdate();
                connection.commit();
            } catch (SQLException e) {
                logger.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtilNew.closeDBResources(preparedStatement, null);
            }
        }

        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException {
            //将kafka内的json数据转为map
            Map<String, Object> dataMap = JSONObject.parseObject(record.getColumn(7).getRawData().toString().toLowerCase(Locale.ROOT));
            //将map按照插入表的字段转为 list
            List<Object> columnList = new ArrayList<>();
            for (String column : this.columns) {
                columnList.add(dataMap.get(column.toLowerCase(Locale.ROOT)));
            }
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                String typeName = this.resultSetMetaData.getRight().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, typeName, columnList.get(i));
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, String typeName, Object object) throws SQLException {
            java.util.Date utilDate;

            if (null == object) {
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
                    preparedStatement.setString(columnIndex + 1, String.valueOf(object));
                    break;
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = String.valueOf(object);
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = Long.valueOf(String.valueOf(object));
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
                    Long objectLong = Long.valueOf(String.valueOf(object));
                    if (typeName.equalsIgnoreCase("year")) {
                        if (objectLong == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement.setInt(columnIndex + 1, objectLong.intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = new Date(object.toString());
                        } catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", object));
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
                        utilDate = new Date(object.toString());
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", object));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = new Date(object.toString());
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", object));
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
                    preparedStatement.setBytes(columnIndex + 1, (byte[]) object);
                    break;

                case Types.BOOLEAN:
                    preparedStatement.setString(columnIndex + 1, object.toString());
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1, (Boolean) object);
                    } else {
                        preparedStatement.setString(columnIndex + 1, object.toString());
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

        protected void doOneInsert(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
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
                DBUtilNew.closeDBResources(preparedStatement, null);
            }
        }


        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean supportFailOver() {
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }
    }
}
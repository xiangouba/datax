package com.alibaba.datax.plugin.writer.sgccwriter.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.*;

import static com.alibaba.datax.plugin.rdbms.util.DBUtil.delete;
import static com.alibaba.datax.plugin.rdbms.util.DBUtil.query;

/**
 * @Author gxx
 * @Date 2023年06月15日17时45分
 */
public class CommonWriter {
    public static class Job {
        private DataBaseType dataBaseType;

        private static final Logger logger = LoggerFactory
                .getLogger(CommonRdbmsWriter.Job.class);

        public Job(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
            OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
        }

        public void init(Configuration originalConfig) {
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            logger.debug("After job init(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        /*目前只支持MySQL Writer跟Oracle Writer;检查PreSQL跟PostSQL语法以及insert，delete权限*/
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查PreSql跟PostSql语句*/
            prePostSqlValid(originalConfig, dataBaseType);
            /*检查insert 跟delete权限*/
            privilegeValid(originalConfig, dataBaseType);
        }

        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查PreSql跟PostSql语句*/
            WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
            WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);
        }

        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查insert 跟delete权限*/
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                boolean hasInsertPri = DBUtil.checkInsertPrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);

                if (!hasInsertPri) {
                    throw RdbmsException.asInsertPriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                }

                if (DBUtil.needCheckDeletePrivilege(originalConfig)) {
                    boolean hasDeletePri = DBUtil.checkDeletePrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);
                    if (!hasDeletePri) {
                        throw RdbmsException.asDeletePriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                    }
                }
            }
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        public void prepare(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                        Object.class);
                Configuration connConf = Configuration.from(conns.get(0)
                        .toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                originalConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                originalConfig.set(Key.TABLE, table);

                List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);

                originalConfig.remove(Constant.CONN_MARK);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    originalConfig.remove(Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                    logger.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            logger.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public List<Configuration> split(Configuration originalConfig,
                                         int mandatoryNumber) {
            return WriterUtil.doSplit(originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                // 已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

                String table = originalConfig.getString(Key.TABLE);

                List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                        String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                        postSqls, table);

                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    // 说明有 postSql 配置，则此处删除掉
                    originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType,
                            jdbcUrl, username, password);

                    logger.info(
                            "Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        public void destroy(Configuration originalConfig) {
        }

    }

    public static class Task {
        protected static final Logger logger = LoggerFactory.getLogger(CommonRdbmsWriter.Task.class);

        protected DataBaseType dataBaseType;
        private static final String VALUE_HOLDER = "?";

        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected List<String> columns;
        protected List<Map> columnMap;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected Boolean deleteFlag;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;

        protected static String INSERT_OR_REPLACE_TEMPLATE;

        protected String writeRecordSql;
        protected String recordSql;
        protected String deleteRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        public Task(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration writerSliceConfig) {
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);

            logger.info("--------------- write init : " + username + "  " + password + "  " + jdbcUrl + "   " + dataBaseType);

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
            this.columnMap = writerSliceConfig.getList(Key.COLUMN_MAP, Map.class);
            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            //sgcc的处理
            if (this.dataBaseType == DataBaseType.Sgcc){
                this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.SGCC_BATCH_SIZE);
            }
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            this.deleteFlag = (null != writerSliceConfig.getBool(Key.DELETE_FLAG)) ? writerSliceConfig.getBool(Key.DELETE_FLAG) : false;
            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        public void prepare(Configuration writerSliceConfig) {
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

        public void startWriteWithConnection(RecordReceiver recordReceiver, RecordSender recordSender,TaskPluginCollector taskPluginCollector, Connection connection) throws SQLException {
            this.taskPluginCollector = taskPluginCollector;

            //查询本地备份数据库数据 TODO
            //本地数据库表名
            String backTable = this.table;
            //查询record记录表
            // TODO 可以将该连接信息改造至json模板中
            Connection recordConnection = DBUtil.getConnection(DataBaseType.Oracle,
                    "jdbc:oracle:thin:@10.1.209.79:21521/ydthsjb", "ydthsjb", "svXSxAGNztKx8Qu");
            StringBuilder backQuerySql = new StringBuilder();
            backQuerySql.append("select table_name,md5code,opt from RS_RECORD where TABLE_NAME = '").append(this.table).append("'");

            StringBuilder backDeleteSql = new StringBuilder();
            backDeleteSql.append("delete from ").append(backTable).append("  where id= '%s' and TABLE_NAME = '").append(this.table).append("'");

            ResultSet rs = null;

            try {
                rs = query(recordConnection, backQuerySql.toString(), Constants.DEFAULT_FETCH_SIZE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            //备份库查询到的数据集合 mapList
            List<Map<String,String>> mapList = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            //将查询结果放入 mapList
            while (rs.next()){
                Map<String,String> mapOfColumnValues = new HashMap(count);
                mapOfColumnValues.put("table_name",rs.getString(1));
                mapOfColumnValues.put("md5code",rs.getString(2));
                mapOfColumnValues.put("opt",rs.getString(3));
                mapList.add(mapOfColumnValues);
            }
            // record数据整理
            //删除的数据 id
            List<String> deleteList = new ArrayList<>();
            //新增的数据 id
            List<String> insertList = new ArrayList<>();
            for (Map<String, String> map : mapList) {
                if (map.get("opt").equals("delete")){
                    deleteList.add(map.get("md5code"));
                }else {
                    insertList.add(map.get("md5code"));
                }
            }
            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                    this.table, StringUtils.join(this.columns, ","));

            logger.info("删除的数据条数：{}",deleteList.size());
            //删除操作
            for (String s : deleteList) {
                Statement stmt = null;
                String currentSql = null;
                try {
                    currentSql = String.format(backDeleteSql.toString(),s);
                    stmt = connection.createStatement();
                    DBUtil.executeSqlWithoutResultSet(stmt, currentSql);
                } catch (Exception e) {
                    throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
                } finally {
                    DBUtil.closeDBResources(null, stmt, null);
                }
//                delete(connection, String.format(backDeleteSql.toString(),s), Constants.DEFAULT_FETCH_SIZE);
            }
            logger.info("新增的数据条数：{}",insertList.size());
            //新增操作
            insertDateHandle(recordReceiver,recordSender,connection, insertList);

            //删除记录表

            int deleteCount = delete(recordConnection,"delete from RS_RECORD",Constants.DEFAULT_FETCH_SIZE);
            logger.info("删除记录表数据：{}条",deleteCount);
            //关闭连接
            DBUtil.closeDBResources(null, recordConnection);

        }

        private List<Record> buildRecord(RecordSender recordSender,List<String> insertList,List<Map> newList){
            List<Record> resBuffer = new ArrayList<>();
            for (Map map : newList) {
                if (insertList.contains(map.get("id"))){
                    //插入
                    Record record = recordSender.createRecord();
                    for (Map map1 : this.columnMap) {
                        if (map1.get("columnType").equals("String")){
                            record.addColumn(new StringColumn(map.get(map1.get("columnName")) + ""));
                        }else{
                            String columnName = map1.get("columnName").toString();
                            record.addColumn(new DateColumn((Date) map.get(columnName)) );
                        }
                    }
                    record.addColumn(new StringColumn(map.get("id").toString()));
                    resBuffer.add(record);
                }
            }
            return resBuffer;
        }

        /**
         *  将读插件查询到的数据组装到newList
         * @param newMap 新数据的map
         * @param record 数据源
         */
        private void newDataHandle( List<Map> newMap, Record record) throws SQLException {
            HashMap<String, Object> map = new HashMap<>();
            map = recordToJsonString(this.columnMap, record);
            newMap.add(map);
        }

        /**
         *  业务员信息逻辑处理
         * @param recordReceiver
         * @param connection
         * @param insertList
         * @throws SQLException
         */
        private void insertDateHandle(RecordReceiver recordReceiver,RecordSender recordSender, Connection connection, List<String> insertList) throws SQLException {
            //读插件查询到的全部数据
            List<Map> newList = new ArrayList<>();
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
                    newDataHandle(newList, record);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
            List<Record> resBuffer = new ArrayList<>();
            //增量记录
            resBuffer = buildRecord(recordSender,insertList,newList);

            doBatchInsert(connection, resBuffer);
        }


        private HashMap<String, Object> recordToJsonString(List<Map> columnMap,Record record) throws SQLException {
            int recordLength = record.getColumnNumber();
            HashMap<String, Object> map = new HashMap<>(16);
            for (int i = 0; i < recordLength; i++) {
                //获取columnName
                String columnName = columnMap.get(i).get(Constants.COLUMN_NAME).toString();
                //获取columnType
                String columnType = columnMap.get(i).get(Constants.COLUMN_TYPE).toString();
                //获取column
                Column column = record.getColumn(i);
                putValueToMap(columnName, columnType, column, map);
            }
            return map;
        }

        /**
         * 根据columnType设置对应类型的值
         *
         * @param columnName
         * @param columnType
         * @param column
         * @param map
         */
        private void putValueToMap(String columnName, String columnType, Column column, Map<String, Object> map) throws SQLException {
            java.util.Date utilDate;
            switch (columnType) {
                case "String":
                    map.put(columnName, column.asString());
                    break;
                case "Integer":
                    map.put(columnName, column.asBigInteger());
                    break;
                case "Long":
                    map.put(columnName, column.asLong());
                    break;
                case "Byte":
                    map.put(columnName, column.asBytes());
                    break;
                case "Boolean":
                    map.put(columnName, column.asBoolean());
                    break;
                case "Date":
                    map.put(columnName, column.asDate());
                    break;
                case "Time":
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
                    map.put(columnName,sqlTime);
                    break;
                case "Double":
                    map.put(columnName, column.asDouble());
                    break;
                case "Decimal":
                    map.put(columnName, column.asBigDecimal());
                    break;
                default:
                    throw DataXException.asDataXException(
                            "[column.columnType]有误：" + columnType);
            }
        }

        // TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver, RecordSender recordSender,
                               Configuration writerSliceConfig,
                               TaskPluginCollector taskPluginCollector) throws SQLException {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                    this.dataBaseType, BASIC_MESSAGE);
            startWriteWithConnection(recordReceiver, recordSender,taskPluginCollector, connection);
        }


        public void post(Configuration writerSliceConfig) {
            int tableNumber = writerSliceConfig.getInt(
                    Constant.TABLE_NUMBER_MARK);

            boolean hasPostSql = (this.postSqls != null && this.postSqls.size() > 0);
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);

            logger.info("Begin to execute postSqls:[{}]. context info:{}.",
                    StringUtils.join(this.postSqls, ";"), BASIC_MESSAGE);
            WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE, dataBaseType);
            DBUtil.closeDBResources(null, null, connection);
        }

        public void destroy(Configuration writerSliceConfig) {
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
                DBUtil.closeDBResources(preparedStatement, null);
            }
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
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        /**
         *  本地备份表做数据记录
         * @param connection
         * @param buffer
         */
        protected void doOneInsertRecord(Connection connection, List<Record> buffer) {

            List<String> valueHolders = new ArrayList<String>(3);
            valueHolders.add("?");
            valueHolders.add("?");
            valueHolders.add("?");
            StringBuilder writeDataSqlTemplate = new StringBuilder();
            writeDataSqlTemplate.append("INSERT")
                    .append(" INTO %s (").append("table_name,md5code,opt")
                    .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                    .append(")").toString();
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection
                        .prepareStatement(String.format(writeDataSqlTemplate.toString(), this.table));

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
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        // 直接使用了两个类变量：columnNumber,resultSetMetaData
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                String typeName = this.resultSetMetaData.getRight().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, typeName, record.getColumn(i));
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, Column column) throws SQLException {
            return fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqltype, null, column);
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, String typeName, Column column) throws SQLException {
            Date utilDate;

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
                    Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new Timestamp(
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
                    if (this.dataBaseType == DataBaseType.MySql) {
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
    }
}

package com.alibaba.datax.plugin.writer.sjsqwriter.util;

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
import com.alibaba.datax.plugin.writer.sjsqwriter.entity.*;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.image.ImageWatched;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

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
            this.columnMap = writerSliceConfig.getList(Key.COLUMN_MAP, Map.class);
            this.columnNumber = this.columns.size();

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

        public void startWriteWithConnection(RecordReceiver recordReceiver,RecordSender recordSender, TaskPluginCollector taskPluginCollector, Connection connection) throws SQLException {
            this.taskPluginCollector = taskPluginCollector;

            //查询本地备份数据库数据 TODO
            //本地数据库表名
            String backTable = this.table;
            StringBuilder backQuerySql = new StringBuilder();
            backQuerySql.append("select id from ").append(backTable);

            StringBuilder backDeleteSql = new StringBuilder();
            backDeleteSql.append("delete from ").append(backTable).append("  where id= '%s'");

            ResultSet rs = null;

            try {
                rs = query(connection, backQuerySql.toString(), Constants.DEFAULT_FETCH_SIZE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            //备份库查询到的数据集合 mapList
            List<Map<String,String>> mapList = new ArrayList<>();
            //旧数据的map
            Map<String,List<String>> oldMap = new HashMap<>();
            //将查询结果放入 mapList
            while (rs.next()){
                Map<String,String> mapOfColumnValues = new HashMap();
                mapOfColumnValues.put("ID",rs.getObject(1).toString());
                mapList.add(mapOfColumnValues);
            }
            // 旧数据
            for (Map<String, String> map : mapList) {
                if (oldMap.get(map.get("ID")) == null) {
                    List<String> list = new ArrayList<>();
                    list.add(map.get("ID"));
                    oldMap.put(map.get("ID").toString(),list);
                } else {
                    oldMap.get(map.get("ID")).add(map.get("ID"));
                }
            }
            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                    this.table, StringUtils.join(this.columns, ","));
            /*
             * 1.查询整理新数据
             * 2.新旧数据比对后，做相应操作
             * */
            switch (this.table){
                case "qryj_z_rs_salesman_info":
                    handleSalesmanNewDate(recordReceiver,recordSender, connection, backDeleteSql, oldMap);
                    break;
                case "qryj_z_rs_claim_info":
                    handleClaimNewDate(recordReceiver, recordSender,connection, backDeleteSql, oldMap);
                    break;
                case "qryj_z_rs_un_contract_info":
                    handleContractNewDate(recordReceiver, recordSender,connection, backDeleteSql, oldMap);
                    break;
                case "qryj_z_rs_underwriting_info":
                    handleCoverageNewDate(recordReceiver, recordSender,connection, backDeleteSql, oldMap);
                    break;
                default:
                    break;
            }
        }

        private void handleCoverageNewDate(RecordReceiver recordReceiver,RecordSender recordSender, Connection connection, StringBuilder backDeleteSql, Map<String, List<String>> oldMap) throws SQLException{
            Map<String, List<RsUnderwritingInfo>> newMap = new HashMap<>();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    coverageNewData(format, newMap, record);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }

            DeltaPair deltaPair = DataUtile.receiveDefectList_3(oldMap, newMap);
            //差集(须删除的Id)
            List<String> oldCjList = deltaPair.getDelete();

            //差集(须新增的Id)
            Set<String> newCjList = deltaPair.getInsert();

            //增量记录
            List<RsRecord> recordList = new ArrayList<>();
            if (oldCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordList(DataUtile.table_rs_underwriting_info, oldCjList, "delete"));
                //根据增量数据 删除原数据的信息
                logger.info("删除的数据条数：{}",oldCjList.size());
                for (String md5 : oldCjList) {
                    Statement stmt = null;
                    String currentSql = null;
                    try {
                        currentSql = String.format(backDeleteSql.toString(), md5);
                        stmt = connection.createStatement();
                        DBUtil.executeSqlWithoutResultSet(stmt, currentSql);
                    } catch (Exception e) {
                        throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
                    } finally {
                        DBUtil.closeDBResources(null, stmt, null);
                    }
//                    delete(connection, String.format(backDeleteSql.toString(),md5), Constants.DEFAULT_FETCH_SIZE);
                }
            }
            List<Record> resBuffer = new ArrayList<>();
            List<Record> recordBuffer = new ArrayList<>();
            if (newCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordSet(DataUtile.table_rs_underwriting_info, newCjList, "insert"));
                //新增
                logger.info("新增的数据条数：{}",newCjList.size());
                for (String md5 : newCjList) {
                    for (RsUnderwritingInfo rs : newMap.get(md5)) {
                        Record record = recordSender.createRecord();
                        record.addColumn(new StringColumn(rs.getId()));
                        record.addColumn(new StringColumn(rs.getCompany_name()));
                        record.addColumn(new StringColumn(rs.getCompany_id()));
                        record.addColumn(new StringColumn(rs.getPolicy_num()));
                        record.addColumn(new DateColumn(rs.getPolicy_signing_date()));
                        record.addColumn(new DateColumn(rs.getReceipt_date()));
                        record.addColumn(new DateColumn(rs.getUnderwriting_time()));
                        record.addColumn(new StringColumn(rs.getPolicy_number2()));
                        record.addColumn(new StringColumn(rs.getSalesman_id()));
                        record.addColumn(new StringColumn(rs.getG_version()));
                        record.addColumn(new DateColumn(rs.getG_create_time()));
                        resBuffer.add(record);
                    }
                }
                logger.info("开始插入！！！！");
                doBatchInsert(connection, resBuffer);
            }
            //新增增量记录
            recordInsert(connection, recordSender,recordList, recordBuffer);
        }
        private void coverageNewData(SimpleDateFormat format, Map<String, List<RsUnderwritingInfo>> newMap, Record record) throws SQLException {
            RsUnderwritingInfo contractInfo = null;
            HashMap<String, Object> map = new HashMap<>();
            map = recordToJsonString(this.columnMap, record);
            try {
                String company_id = (String) map.get("单位编号");
                String company_name = (String) map.get("单位名称");
                String policy_num = (String) map.get("保单号");
//                String policy_signing_date2 =  != null ? format.format((Timestamp) map.get("投保日期")) : null;
                Date policy_signing_date = (Date) map.get("投保日期");
//                String receipt_date2 = map.get("回执签收日期") != null ? format.format((Timestamp) map.get("回执签收日期")) : null;
                Date receipt_date = (Date) map.get("回执签收日期");
                Date underwriting_time = (Date) map.get("承保时间");
                String underwriting_time2 = underwriting_time != null ? format.format(underwriting_time) : null;
                String salesman_id = (String) map.get("业务员编码");
                String policy_number2 = (String) map.get("投保单号");
                Date g_create_time = (Date) map.get("抽数时间");
                String g_create_time2 = g_create_time != null ? format.format(g_create_time) : null;
                String g_version = (String) map.get("系统版本");

                contractInfo = new RsUnderwritingInfo(company_name, company_id, policy_num, policy_signing_date, receipt_date, underwriting_time, policy_number2, salesman_id, g_version, g_create_time);
                if (newMap.get(contractInfo.getId()) == null) {
                    List<RsUnderwritingInfo> list = new ArrayList<>();
                    list.add(contractInfo);
                    newMap.put(contractInfo.getId(), list);
                } else {
                    newMap.get(contractInfo.getId()).add(contractInfo);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }

        /**
         * 承保契调
         * @param recordReceiver
         * @param connection
         * @param backDeleteSql
         * @param oldMap
         * @throws SQLException
         */
        private void handleContractNewDate(RecordReceiver recordReceiver,RecordSender recordSender, Connection connection, StringBuilder backDeleteSql, Map<String, List<String>> oldMap) throws SQLException{
            Map<String, List<RsUnderwritingContractInfo>> newMap = new HashMap<>();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    contractNewData(format, newMap, record);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }

            DeltaPair deltaPair = DataUtile.receiveDefectList_1(oldMap, newMap);
            //差集(须删除的Id)
            List<String> oldCjList = deltaPair.getDelete();

            //差集(须新增的Id)
            Set<String> newCjList = deltaPair.getInsert();

            //增量记录
            List<RsRecord> recordList = new ArrayList<>();
            if (oldCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordList(DataUtile.table_rs_underwriting_contract_info, oldCjList, "delete"));
                //根据增量数据 删除原数据的信息
                logger.info("删除的数据条数：{}",oldCjList.size());
                for (String md5 : oldCjList) {
                    Statement stmt = null;
                    String currentSql = null;
                    try {
                        currentSql = String.format(backDeleteSql.toString(), md5);
                        stmt = connection.createStatement();
                        DBUtil.executeSqlWithoutResultSet(stmt, currentSql);
                    } catch (Exception e) {
                        throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
                    } finally {
                        DBUtil.closeDBResources(null, stmt, null);
                    }
//                    delete(connection, String.format(backDeleteSql.toString(),md5), Constants.DEFAULT_FETCH_SIZE);
                }
            }
            List<Record> resBuffer = new ArrayList<>();
            List<Record> recordBuffer = new ArrayList<>();
            if (newCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordSet(DataUtile.table_rs_underwriting_contract_info, newCjList, "insert"));
                logger.info("新增的数据条数：{}",newCjList.size());
                //新增
                for (String md5 : newCjList) {
                    for (RsUnderwritingContractInfo rs : newMap.get(md5)) {
                        Record record = recordSender.createRecord();
                        record.addColumn(new StringColumn(rs.getId()));
                        record.addColumn(new StringColumn(rs.getCompany_name()));
                        record.addColumn(new StringColumn(rs.getCompany_id()));
                        record.addColumn(new StringColumn(rs.getPolicy_num()));
                        record.addColumn(new DateColumn(rs.getContract_delivery_time()));
                        record.addColumn(new DateColumn(rs.getContract_recovery_time()));
                        record.addColumn(new DateColumn(rs.getUnderwriting_time()));
                        record.addColumn(new StringColumn(rs.getPolicy_number2()));
                        record.addColumn(new StringColumn(rs.getG_version()));
                        record.addColumn(new DateColumn(rs.getG_create_time()));
                        resBuffer.add(record);
                    }
                }
                logger.info("开始插入数据到备份库");
                doBatchInsert(connection, resBuffer);
            }
            //新增增量记录
            recordInsert(connection, recordSender,recordList, recordBuffer);
        }


        /**
         * @param format
         * @param newMap
         * @param record
         */
        private void contractNewData(SimpleDateFormat format, Map<String, List<RsUnderwritingContractInfo>> newMap, Record record) throws SQLException {
            RsUnderwritingContractInfo contractInfo = null;
            HashMap<String, Object> map = new HashMap<>();
            map = recordToJsonString(this.columnMap, record);
            try {
                String company_id = (String) map.get("单位编号");
                String company_name = (String) map.get("单位名称");
                String policy_num = (String) map.get("保单号");
                Date contract_delivery_time = (Date) map.get("下发");
                String contract_delivery_time2 = contract_delivery_time != null ? format.format(contract_delivery_time) : null;
                Date contract_recovery_time = (Date) map.get("回销");
                String contract_recovery_time2 = contract_recovery_time != null ? format.format(contract_recovery_time) : null;
                Date underwriting_time = (Date) map.get("承保时间");
                String underwriting_time2 = contract_recovery_time != null ? format.format(underwriting_time) : null;
                String policy_number2 = (String) map.get("投保单号");
                Date g_create_time = (Date) map.get("抽数时间");
                String g_create_time2 = g_create_time != null ? format.format(g_create_time) : null;
                String g_version = (String) map.get("系统版本");
                contractInfo = new RsUnderwritingContractInfo(company_name, company_id, policy_num, contract_delivery_time, contract_recovery_time, underwriting_time, policy_number2, g_version, g_create_time);
                if (newMap.get(contractInfo.getId()) == null) {
                    List<RsUnderwritingContractInfo> list = new ArrayList<>();
                    list.add(contractInfo);
                    newMap.put(contractInfo.getId(), list);
                } else {
                    newMap.get(contractInfo.getId()).add(contractInfo);
                }

            } catch (Exception e) {
                logger.error("", e);
            }
        }

        /**
         *  理赔信息逻辑处理
         * @param recordReceiver
         * @param connection
         * @param backDeleteSql
         * @param oldMap
         * @throws SQLException
         */
        private void handleClaimNewDate(RecordReceiver recordReceiver,RecordSender recordSender, Connection connection, StringBuilder backDeleteSql, Map<String, List<String>> oldMap) throws SQLException{
            Map<String, List<RsClaimInfo>> newMap = new HashMap<>();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
                    claimNewData(format, newMap, record);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }

            DeltaPair deltaPair = DataUtile.receiveDefectList(oldMap, newMap);
            //差集(须删除的Id)
            List<String> oldCjList = deltaPair.getDelete();
            logger.info("差集，需要删除的条数：{}",oldCjList.size());
            //差集(须新增的Id)
            Set<String> newCjList = deltaPair.getInsert();
            logger.info("差集需要新增的数据条数：{}",newCjList.size());
            //增量记录
            List<RsRecord> recordList = new ArrayList<>();
            if (oldCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordList(DataUtile.table_rs_claim_info, oldCjList, "delete"));
                //根据增量数据 删除原数据的信息
                for (String md5 : oldCjList) {
                    Statement stmt = null;
                    String currentSql = null;
                    try {
                        currentSql = String.format(backDeleteSql.toString(), md5);
                        stmt = connection.createStatement();
                        DBUtil.executeSqlWithoutResultSet(stmt, currentSql);
                    } catch (Exception e) {
                        throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
                    } finally {
                        DBUtil.closeDBResources(null, stmt, null);
                    }
//                    delete(connection, String.format(backDeleteSql.toString(),md5), Constants.DEFAULT_FETCH_SIZE);
                }
            }
            List<Record> resBuffer = new ArrayList<>();
            List<Record> recordBuffer = new ArrayList<>();
            if (newCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordSet(DataUtile.table_rs_claim_info, newCjList, "insert"));
                //新增
                for (String md5 : newCjList) {
                    List<RsClaimInfo> list = newMap.get(md5);
                    for (RsClaimInfo rs : list) {
                        Record record = recordSender.createRecord();
                        record.addColumn(new StringColumn(rs.getCompany_id()));
                        record.addColumn(new StringColumn(rs.getCompany_name()));
//                        record.addColumn(new StringColumn(rs.getPolicy_num()));
                        record.addColumn(new StringColumn(rs.getOtherno()));
                        record.addColumn(new DateColumn(rs.getClaim_settlement_time()));
                        record.addColumn(new DateColumn(rs.getFiling_time()));
                        record.addColumn(new StringColumn(rs.getInvestigation()));
                        record.addColumn(new StringColumn(rs.getClaim_type()));
                        record.addColumn(new DateColumn(rs.getG_create_time()));
                        record.addColumn(new StringColumn(rs.getG_version()));
                        record.addColumn(new StringColumn(rs.getId()));
                        resBuffer.add(record);
                    }
                }
                doBatchInsert(connection, resBuffer);
            }
            //新增增量记录
            recordInsert(connection, recordSender,recordList, recordBuffer);
        }

        /**
         *  理赔信息处理
         * @param format
         * @param newMap
         * @param record
         */
        private void claimNewData(SimpleDateFormat format, Map<String, List<RsClaimInfo>> newMap, Record record) throws SQLException {
            RsClaimInfo rsClaimInfo = null;
            HashMap<String, Object> map = new HashMap<>();
            map = recordToJsonString(this.columnMap, record);
            try {
                String company_id = (String) map.get("单位编号");
                String company_name = (String) map.get("单位名称");
                String policy_num = (String) map.get("保单号");
                String otherno = (String) map.get("赔案号");
                String claim_settlement_time2 = (String) map.get("理赔实付时间");
                Date claim_settlement_time = claim_settlement_time2 != null ? format.parse(claim_settlement_time2) : null;
                String filing_time = (String) map.get("材料提交日期");
                Date filing_time2 = (filing_time != null && !filing_time.trim().isEmpty()) ? format.parse(filing_time) : null;
                String investigation = (String) map.get("调查");
                String claim_type = (String) map.get("理赔类型");
                Date g_create_time = (Date) map.get("抽数时间");
                String g_version = (String) map.get("系统版本");
                rsClaimInfo = new RsClaimInfo(company_name, company_id, policy_num, claim_settlement_time, filing_time2, investigation, claim_type, otherno, g_version, g_create_time);
                if (newMap.get(rsClaimInfo.getId()) == null) {
                    List<RsClaimInfo> list = new ArrayList<>();
                    list.add(rsClaimInfo);
                    newMap.put(rsClaimInfo.getId(), list);
                } else {
                    newMap.get(rsClaimInfo.getId()).add(rsClaimInfo);
                }

            } catch (Exception e) {
                logger.error("", e);
            }
        }

        /**
         *  将读插件查询到的数据组装到newMap
         * @param format 时间格式
         * @param newMap 新数据的map
         * @param record 数据源
         */
        private void salesmanNewData(SimpleDateFormat format, Map<String, List<RsSalesmanInfo>> newMap, Record record) throws SQLException {
            RsSalesmanInfo salesmanInfo = null;
            HashMap<String, Object> map = new HashMap<>();
            //将record转换为map
            map = recordToJsonString(this.columnMap, record);
            //将map转为业务员对象
            salesmanInfo = getSalesman(salesmanInfo, map, format);
            if (newMap.get(salesmanInfo.getId()) == null) {
                List<RsSalesmanInfo> list = new ArrayList<>();
                list.add(salesmanInfo);
                newMap.put(salesmanInfo.getId(), list);
            } else {
                newMap.get(salesmanInfo.getId()).add(salesmanInfo);
            }
        }

        /**
         *  业务员信息逻辑处理
         * @param recordReceiver
         * @param connection
         * @param backDeleteSql
         * @param oldMap
         * @throws SQLException
         */
        private void handleSalesmanNewDate(RecordReceiver recordReceiver, RecordSender recordSender,Connection connection, StringBuilder backDeleteSql, Map<String, List<String>> oldMap) throws SQLException {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map<String, List<RsSalesmanInfo>> newMap = new HashMap<>();
            Map<String, List<RsSalesmanInfo>> newDataMap = new HashMap<>();
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    salesmanNewData(format, newMap, record);
//                    bufferBytes = newMap.size();
//                    if (bufferBytes >= 2048) {
//                        newDataMap.putAll(newMap);
//                        newMap.clear();
//                        logger.info("1.新的数据map有:{}条。",newDataMap.size());
//                    }
//                    if (!newMap.isEmpty()){
//                        newDataMap.putAll(newMap);
//                        logger.info("2.新的数据map有:{}条。",newDataMap.size());
//                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }



            DeltaPair deltaPair = DataUtile.receiveDefectList_2(oldMap, newMap);
            //差集(须删除的Id)
            List<String> oldCjList = deltaPair.getDelete();

            //差集(须新增的Id)
            Set<String> newCjList = deltaPair.getInsert();

            //增量记录
            List<RsRecord> recordList = new ArrayList<>();
            if (oldCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordList(DataUtile.table_rs_salesman_info, oldCjList, "delete"));
                logger.info("差集，需要删除的条数：{}", oldCjList.size());
                //根据增量数据 删除原数据的信息
                for (String md5 : oldCjList) {
                    Statement stmt = null;
                    String currentSql = null;
                    try {
                        currentSql = String.format(backDeleteSql.toString(), md5);
                        stmt = connection.createStatement();
                        DBUtil.executeSqlWithoutResultSet(stmt, currentSql);
                    } catch (Exception e) {
                        throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
                    } finally {
                        DBUtil.closeDBResources(null, stmt, null);
                    }
//                    delete(connection,String.format(backDeleteSql.toString(),md5) , Constants.DEFAULT_FETCH_SIZE);
                }
            }
            List<Record> resBuffer = new ArrayList<>();
            List<Record> recordBuffer = new ArrayList<>();
            if (newCjList.size() > 0) {
                recordList.addAll(DataUtile.getRsRecordSet(DataUtile.table_rs_salesman_info, newCjList, "insert"));
                logger.info("差集需要新增的数据条数：{}",newCjList.size() );
                //新增
                for (String md5 : newCjList) {
                    for (RsSalesmanInfo rsInfo : newMap.get(md5)) {
                        Record record = recordSender.createRecord();
                        record.addColumn(new StringColumn(rsInfo.getSalesman()));
                        record.addColumn(new StringColumn(rsInfo.getBusiness_license_no()));
                        record.addColumn(new DateColumn(rsInfo.getLicense_valid_start_date()));
                        record.addColumn(new DateColumn(rsInfo.getLicense_expiration_date()));
                        record.addColumn(new StringColumn(rsInfo.getSalesman_id()));
                        record.addColumn(new StringColumn(rsInfo.getPolicy_num()));
                        record.addColumn(new DateColumn(rsInfo.getPolicy_signing_date()));
                        record.addColumn(new StringColumn(rsInfo.getPolicy_number2()));
                        record.addColumn(new StringColumn(rsInfo.getComptxt()));
                        record.addColumn(new StringColumn(rsInfo.getCompid()));
                        record.addColumn(new StringColumn(rsInfo.getG_version()));
                        record.addColumn(new DateColumn(rsInfo.getG_create_time()));
                        record.addColumn(new StringColumn(rsInfo.getId()));
                        resBuffer.add(record);
                    }
                }
                doBatchInsert(connection, resBuffer);
            }
            //新增增量记录
            recordInsert(connection, recordSender,recordList, recordBuffer);
        }

        private void recordInsert(Connection connection, RecordSender recordSender,List<RsRecord> recordList, List<Record> recordBuffer) {
            for (RsRecord rsRecordInfo : recordList) {
                Record record = recordSender.createRecord();
                record.addColumn(new StringColumn(rsRecordInfo.getTable_name()));
                record.addColumn(new StringColumn(rsRecordInfo.getMd5code()));
                record.addColumn(new StringColumn(rsRecordInfo.getOpt()));
                recordBuffer.add(record);
            }
            doOneInsertRecord(connection, recordBuffer);
        }

        public RsSalesmanInfo getSalesman(RsSalesmanInfo salesmanInfo, Map<String, Object> map, SimpleDateFormat format) {
            try {
                String salesman = (String) map.get("业务员名称");
                String business_license_no = (String) map.get("执业证编号");
                Date license_valid_start_date = (Date) map.get("执业证有效起期");
                String license_valid_start_date2 = license_valid_start_date != null ? format.format(license_valid_start_date) : null;
                Date license_expiration_date = (Date) map.get("执业证有效止期");
                String license_expiration_date2 = license_expiration_date != null ? format.format(license_expiration_date) : null;
                String salesman_id = (String) map.get("业务员编码");
                String policy_num = (String) map.get("保单号");
                Date policy_signing_date = (Date) map.get("投保日期");
                String policy_number2 = (String) map.get("投保单号");
                String comptxt = (String) map.get("单位名称");
                String compid = (String) map.get("单位编码");
                Date g_create_time = (Date) map.get("抽数时间");
                String g_version = (String) map.get("系统版本");
                salesmanInfo = new RsSalesmanInfo(salesman, business_license_no, license_valid_start_date, license_expiration_date,
                        salesman_id, policy_num, policy_signing_date,policy_number2,  compid,comptxt,g_version, g_create_time);

            } catch (Exception e) {
                logger.error("", e);
            }

            return salesmanInfo;
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
                case "Double":
                    map.put(columnName, column.asDouble());
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
            startWriteWithConnection(recordReceiver, recordSender, taskPluginCollector, connection);
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
                        .prepareStatement(String.format(writeDataSqlTemplate.toString(), "RS_RECORD"));

                for (Record record : buffer) {
                    try {
                        preparedStatement = oneFillPreparedStatement(
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

        protected PreparedStatement oneFillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException {

            for (int i = 0; i < 3; i++) {
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, 12, "VARCHAR2", record.getColumn(i));
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, String typeName, Column column) throws SQLException {
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

        private void calcWriteRecordSql() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
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

                INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, forceUseUpdate);
                writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
            }
        }

        private void calcDeleteRecordSql(){
            String deleteDataSqlTemplate;

            deleteDataSqlTemplate = new StringBuilder().append("DELETE FROM %s WHERE id in ('").toString();
            this.deleteRecordSql = String.format(deleteDataSqlTemplate, this.table);

        }

        protected String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }
    }
}

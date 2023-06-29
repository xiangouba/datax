//package com.alibaba.datax.plugin.reader.hivereader;
//
//import com.alibaba.datax.common.exception.DataXException;
//import com.alibaba.datax.common.plugin.RecordSender;
//import com.alibaba.datax.common.spi.Reader;
//import com.alibaba.datax.common.util.Configuration;
//import com.alibaba.datax.common.util.KeyUtil;
//import com.alibaba.datax.common.util.ShellUtil;
//import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
//import org.apache.commons.lang.StringEscapeUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.time.FastDateFormat;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashSet;
//import java.util.List;
//
///**
// * @author dean 2019/10/25.
// * @version v1.1
// */
//public class HiveReaderOld {
//
//
//    /**
//     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
//     * <p/>
//     * 整个 Reader 执行流程是：
//     * <pre>
//     * Job类init-->prepare-->split
//     *
//     * Task类init-->prepare-->startRead-->post-->destroy
//     * Task类init-->prepare-->startRead-->post-->destroy
//     *
//     * Job类post-->destroy
//     * </pre>
//     */
//
//    public static class Job extends Reader.Job {
//
//        private static final Logger logger = LoggerFactory.getLogger(Job.class);
//        private Configuration readerOriginConfig = null;
//
//
//        @Override
//        public void init() {
//            logger.info("init() begin...");
//            this.readerOriginConfig = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
//            this.validate();
//            logger.info("init() ok and end...");
//            logger.info("HiveReader流程说明[1:Reader的HiveQL导入临时表(TextFile无压缩的HDFS) ;2:临时表的HDFS到目标Writer;3:删除临时表]");
//
//        }
//
//
//        private void validate() {
//
//            this.readerOriginConfig.getNecessaryValue(KeyOld.DEFAULT_FS,
//                    HiveReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
//            List<String> sqls = this.readerOriginConfig.getList(KeyOld.HIVE_SQL, String.class);
//            if (null == sqls || sqls.size() == 0) {
//                throw DataXException.asDataXException(
//                        HiveReaderErrorCode.SQL_NOT_FIND_ERROR,
//                        "您未配置hive sql");
//            }
//            //check Kerberos
//            Boolean haveKerberos = this.readerOriginConfig.getBool(KeyOld.HAVE_KERBEROS, false);
//            if (haveKerberos) {
//                this.readerOriginConfig.getNecessaryValue(KeyOld.KERBEROS_KEYTAB_FILE_PATH, HiveReaderErrorCode.REQUIRED_VALUE);
//                this.readerOriginConfig.getNecessaryValue(KeyOld.KERBEROS_PRINCIPAL, HiveReaderErrorCode.REQUIRED_VALUE);
//            }
//        }
//
//
//        @Override
//        public List<Configuration> split(int adviceNumber) {
//            //按照Hive  sql的个数 获取配置文件的个数
//            logger.info("split() begin...");
//            List<String> sqls = this.readerOriginConfig.getList(KeyOld.HIVE_SQL, String.class);
//            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
//            Configuration splitedConfig = null;
//            for (String querySql : sqls) {
//                splitedConfig = this.readerOriginConfig.clone();
//                splitedConfig.set(KeyOld.HIVE_SQL, querySql);
//                readerSplitConfigs.add(splitedConfig);
//            }
//            return readerSplitConfigs;
//        }
//
//        //全局post
//        @Override
//        public void post() {
//            logger.info("任务执行完毕,hive reader post");
//
//        }
//
//        @Override
//        public void destroy() {
//
//        }
//    }
//
//
//    public static class Task extends Reader.Task {
//
//        private static final Logger logger = LoggerFactory.getLogger(Task.class);
//        private Configuration taskConfig;
//        private String hiveSql;
//        private String tmpPath;
//        private String tableName;
//        private String tempDatabase;
//        private String tempHdfsLocation;
//        private String hive_cmd;
//        private String hive_sql_set;
//        private String fieldDelimiter;
//        private String nullFormat;
//        private String hive_fieldDelimiter;
//        private DFSUtil dfsUtil = null;
//        private HashSet<String> sourceFiles;
//
//        @Override
//        public void init() {
//            this.tableName = hiveTableName();
//            //获取配置
//            this.taskConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件
//            this.hiveSql = taskConfig.getString(KeyOld.HIVE_SQL);//获取hive sql
//            this.tempDatabase = taskConfig.getString(KeyOld.TEMP_DATABASE, Constant.TEMP_DATABASE_DEFAULT);
//            this.tempHdfsLocation = taskConfig.getString(KeyOld.TEMP_DATABASE_HDFS_LOCATION, Constant.TEMP_DATABSE_HDFS_LOCATION_DEFAULT);
//            this.hive_cmd = taskConfig.getString(KeyOld.HIVE_CMD, Constant.HIVE_CMD_DEFAULT);
//            this.hive_sql_set = taskConfig.getString(KeyOld.HIVE_SQL_SET, Constant.HIVE_SQL_SET_DEFAULT);
//            //判断set语句的结尾是否是分号，不是给加一个
//            if (!this.hive_sql_set.trim().endsWith(";")) {
//                this.hive_sql_set = this.hive_sql_set + ";";
//            }
//
//            this.fieldDelimiter = taskConfig.getString(KeyOld.FIELD_DELIMITER, Constant.FIELDDELIMITER_DEFAULT);
//            this.hive_fieldDelimiter = this.fieldDelimiter;
//
//            this.fieldDelimiter = StringEscapeUtils.unescapeJava(this.fieldDelimiter);
//            this.taskConfig.set(KeyOld.FIELD_DELIMITER, this.fieldDelimiter);//设置hive 存储文件 hdfs默认的分隔符,传输时候会分隔
//
//            this.nullFormat = taskConfig.getString(KeyOld.NULL_FORMAT, Constant.NULL_FORMAT_DEFAULT);
//            this.taskConfig.set(KeyOld.NULL_FORMAT, this.nullFormat);
//            //判断set语句的结尾是否是分号，不是给加一个
//            if (!this.tempHdfsLocation.trim().endsWith("/")) {
//                this.tempHdfsLocation = this.tempHdfsLocation + "/";
//            }
//            this.tmpPath = this.tempHdfsLocation + this.tableName;//创建临时Hive表 存储地址
//            logger.info("配置分隔符后:" + this.taskConfig.toJSON());
//            this.dfsUtil = new DFSUtil(this.taskConfig);//初始化工具类
//
//        }
//
//
//        @Override
//        public void prepare() {
//
//            //TODO...创建临时存储路径
//            mkdirTempDatabasePath();
//
//            //创建临时Hive表,指定存储地址
//            String hiveQueryCmd = this.hive_sql_set + " use " + this.tempDatabase + "; create table "
//                    + this.tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + this.hive_fieldDelimiter
//                    + "' STORED AS TEXTFILE "
//                    + " as " + this.hiveSql;
//            logger.info("hiveCmd ----> :" + hiveQueryCmd);
//
//            //执行脚本,创建临时表
//            if (!ShellUtil.exec(new String[]{this.hive_cmd, "-e", "\"" + hiveQueryCmd + " \""})) {
//                throw DataXException.asDataXException(
//                        HiveReaderErrorCode.SHELL_ERROR,
//                        "创建hive临时表脚本执行失败");
//            }
//
//            logger.info("创建hive 临时表结束 end!!!");
//            logger.info("prepare(), start to getAllFiles...");
//            List<String> path = new ArrayList<String>();
//            path.add(tmpPath);
//            this.sourceFiles = dfsUtil.getAllFiles(path, Constant.TEXT);
//            logger.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]",
//                    this.sourceFiles.size(),
//                    StringUtils.join(this.sourceFiles, ",")));
//        }
//
//        @Override
//        public void startRead(RecordSender recordSender) {
//            //读取临时hive表的hdfs文件
//            logger.info("read start");
//            for (String sourceFile : this.sourceFiles) {
//                logger.info(String.format("reading file : [%s]", sourceFile));
//
//                //默认读取的是TEXT文件格式
//                InputStream inputStream = dfsUtil.getInputStream(sourceFile);
//                UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
//                if (recordSender != null) {
//                    recordSender.flush();
//                }
//            }
//            logger.info("end read source files...");
//        }
//
//
//        //只是局部post  属于每个task
//        @Override
//        public void post() {
//            logger.info("one task hive read post...");
//            deleteTmpTable();
//        }
//
//        @Override
//        public void destroy() {
//            logger.info("hive read destroy...");
//        }
//
//
//        /**
//         * 创建hive临时表名称
//         *
//         * @return
//         */
//        private String hiveTableName() {
//
//            StringBuilder str = new StringBuilder();
//            FastDateFormat fdf = FastDateFormat.getInstance("yyyyMMdd");
//
//            str.append(Constant.TEMP_TABLE_NAME_PREFIX).append(fdf.format(new Date())).append("_").append(KeyUtil.genUniqueKey());
//
//            return str.toString().toLowerCase();
//        }
//
//        private void deleteTmpTable() {
//
//            String hiveCmd = this.hive_sql_set + " use " + this.tempDatabase + "; drop table " + this.tableName;
//            logger.info("清空数据:hiveCmd ----> :" + hiveCmd);
//            //执行脚本,创建临时表
//            if (!ShellUtil.exec(new String[]{this.hive_cmd, "-e", "\"" + hiveCmd + "\""})) {
//                throw DataXException.asDataXException(
//                        HiveReaderErrorCode.SHELL_ERROR,
//                        "删除hive临时表脚本执行失败");
//            }
//
//        }
//
//        /**
//         * 创建临时存储路径
//         */
//        public void mkdirTempDatabasePath() {
//            dfsUtil.mkdirDirectory(this.tempHdfsLocation);
//        }
//    }
//
//
//}

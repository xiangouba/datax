package com.alibaba.datax.plugin.reader.chongqingreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.chongqingreader.util.CommonReader;
import com.alibaba.datax.plugin.reader.chongqingreader.util.Constants;
import com.alibaba.datax.plugin.reader.chongqingreader.util.OracleReaderErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author gxx
 * @Date 2023年06月09日14时49分
 */
public class ChongQingReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job extends Reader.Job {
        private static final Logger logger = LoggerFactory.getLogger(ChongQingReader.Job.class);

        private Configuration originalConfig = null;
        private CommonReader.Job commonReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            dealFetchSize(this.originalConfig);

            this.commonReaderJob = new CommonReader.Job(DATABASE_TYPE);
            this.commonReaderJob.init(this.originalConfig);

            // 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
            dealHint(this.originalConfig);
        }

        @Override
        public void preCheck(){
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonReaderJob.split(this.originalConfig,
                    adviceNumber);
        }

        @Override
        public void post() {
            this.commonReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonReaderJob.destroy(this.originalConfig);
        }

        private void dealFetchSize(Configuration originalConfig) {
            int fetchSize = originalConfig.getInt(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    1024);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    fetchSize);
        }

        private void dealHint(Configuration originalConfig) {
            String hint = originalConfig.getString(Key.HINT);
            if (StringUtils.isNotBlank(hint)) {
                boolean isTableMode = originalConfig.getBool(Constants.IS_TABLE_MODE).booleanValue();
                if(!isTableMode){
                    throw DataXException.asDataXException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 时才能配置 HINT.");
                }
                HintUtil.initHintConf(DATABASE_TYPE, originalConfig);
            }
        }
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonReader.Task commonReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonReaderTask = new CommonReader.Task(
                    DATABASE_TYPE ,super.getTaskGroupId(), super.getTaskId());
            this.commonReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig
                    .getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonReaderTask.startRead(this.readerSliceConfig,
                    recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonReaderTask.destroy(this.readerSliceConfig);
        }

    }
}

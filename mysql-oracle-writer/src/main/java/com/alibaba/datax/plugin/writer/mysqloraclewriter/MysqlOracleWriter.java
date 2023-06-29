package com.alibaba.datax.plugin.writer.mysqloraclewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.mysqloraclewriter.util.CommonUtil;

import java.util.List;

/**
 * @Author gxx
 * @Date 2023年06月08日10时55分
 */
public class MysqlOracleWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE1 = DataBaseType.MySql;
    private static final DataBaseType DATABASE_TYPE2= DataBaseType.Oracle;
    public static class Job extends Writer.Job{

        private Configuration originalConfig = null;

        private CommonUtil.Job commonUtilJob;
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonUtilJob = new CommonUtil.Job(DATABASE_TYPE1,DATABASE_TYPE2);
            this.commonUtilJob.init(this.originalConfig);
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return null;
        }
    }


    public static class Task extends Writer.Task{

        private Configuration writerSliceConfig;
        private CommonUtil.Task commonUtilTask;
        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonUtilTask = new CommonUtil.Task(DATABASE_TYPE1,DATABASE_TYPE2);
            this.commonUtilTask.init(this.writerSliceConfig);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver, RecordSender recordSender) throws Exception {
            this.commonUtilTask.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }
    }

}

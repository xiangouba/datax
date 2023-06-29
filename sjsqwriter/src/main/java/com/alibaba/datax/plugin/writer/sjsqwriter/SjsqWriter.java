package com.alibaba.datax.plugin.writer.sjsqwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.sjsqwriter.util.CommonWriter;

import java.util.List;

/**
 * @Author gxx
 * @Date 2023年06月15日17时40分
 */
public class SjsqWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job extends Writer.Job{

        private Configuration originalConfig = null;
        private CommonWriter.Job commonRdbmsWriterJob;
        @Override
        public void preCheck(){
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }
        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }
        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }
    }

    public static class Task extends Writer.Task{
        private Configuration writerSliceConfig;
        private CommonWriter.Task commonWriterTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonWriterTask = new CommonWriter.Task(DATABASE_TYPE);
            this.commonWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonWriterTask.prepare(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public void post() {
            this.commonWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver,RecordSender recordSender) throws Exception {
            this.commonWriterTask.startWrite(recordReceiver, recordSender, this.writerSliceConfig, super.getTaskPluginCollector());

        }
    }
}

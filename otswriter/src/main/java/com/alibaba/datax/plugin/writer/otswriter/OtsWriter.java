package com.alibaba.datax.plugin.writer.otswriter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;

public class OtsWriter {
    public static class Job extends Writer.Job {
        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        
        @Override
        public void init() {
            logger.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (OTSException e) {
                logger.error("OTSException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                logger.error("ClientException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                logger.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                logger.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, Common.getDetailMessage(e), e);
            }
            logger.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            try {
                return this.proxy.split(mandatoryNumber);
            } catch (Exception e) {
                logger.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, Common.getDetailMessage(e), e);
            }
        }
    }
    
    public static class Task extends Writer.Task {
        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private OtsWriterSlaveProxy proxy = new OtsWriterSlaveProxy();
        
        @Override
        public void init() {}

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            logger.info("startWrite() begin ...");
            try {
                this.proxy.init(this.getPluginJobConf());
                this.proxy.write(lineReceiver, this.getTaskPluginCollector());
            } catch (OTSException e) {
                logger.error("OTSException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                logger.error("ClientException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                logger.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                logger.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, Common.getDetailMessage(e), e);
            }
            logger.info("startWrite() end ...");
        }
    }
}

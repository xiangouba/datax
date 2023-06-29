package com.alibaba.datax.plugin.writer.zhbbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.zhbbwriter.constants.ConfigParam;
import com.alibaba.datax.plugin.writer.zhbbwriter.constants.MethodEnum;
import com.alibaba.datax.plugin.writer.zhbbwriter.util.Post;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.alibaba.datax.plugin.writer.zhbbwriter.constants.Constant.*;

/**
 * @Author gxx
 * @Date 2023年06月06日11时14分
 */
public class ZhbbWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            logger.debug("job init begin...");
            this.originalConfig = super.getPluginJobConf();
            String url = this.originalConfig.getString(ConfigParam.URL);
            String method = this.originalConfig.getString(ConfigParam.METHOD);
        }

        @Override
        public void destroy() {

        }
        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> writerSplitConfigurations = new ArrayList<>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration writerSplitConfiguration = this.originalConfig.clone();
                writerSplitConfigurations.add(writerSplitConfiguration);
            }
            return writerSplitConfigurations;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
        protected int batchSize = DEFAULT_BATCH_SIZE;
        protected int batchByteSize = DEFAULT_BATCH_BYTE_SIZE;
        protected String url;
        protected String method;
        protected String isKafka;
        protected List<Map> columnMap;;
        protected int columnNumber = 0;

        @Override
        public void init() {
            logger.debug("task init begin...");
            this.writerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver, RecordSender recordSender) throws Exception {
            this.url = this.writerSliceConfig.getString(ConfigParam.URL);
            this.method = this.writerSliceConfig.getString(ConfigParam.METHOD);
            this.isKafka = this.writerSliceConfig.getString(ConfigParam.IS_KAFKA);
            this.columnMap = this.writerSliceConfig.getList(ConfigParam.COLUMN_MAP, Map.class);
            this.columnNumber = this.columnMap.size();
            //获取datax的数据
            List<Record> pushDataBuffer = new ArrayList<>();
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    bufferBytes += record.getMemorySize();
                    pushDataBuffer.add(record);
                    if (pushDataBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        if (pushDataBuffer != null && !pushDataBuffer.isEmpty()) {
                            /* 数据来源有两种，所以做两种同步策略
                             * 1.kafka消费数据
                             * 2.数据库查询数据
                             * */
                            if (IS_KAFKA.equals(isKafka)){
                                for (Record buffer : pushDataBuffer) {
                                    handleDataIsKafka(buffer);
                                }
                            }else {
                                handleDataIsNotKafka(pushDataBuffer);
                            }
                        }
                        pushDataBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (pushDataBuffer != null && !pushDataBuffer.isEmpty()) {
                    if (IS_KAFKA.equals(isKafka)){
                        for (Record buffer : pushDataBuffer) {
                            handleDataIsKafka(buffer);
                        }
                    }else {
                        handleDataIsNotKafka(pushDataBuffer);
                    }
                    pushDataBuffer.clear();
                }
                bufferBytes = 0;

            } finally {
                pushDataBuffer.clear();
                bufferBytes = 0;
            }
        }

        /**
         * 推送数据
         * @param record
         * @throws Exception
         */
        private void handleDataIsKafka(Record record) throws Exception {
            Post post = new Post();
            //数据格式转换处理
            Map<String, Object> bodyParam = recordToJsonString(record);
            //记录推送数据的返回结果
            Map<String, String> results = new HashMap<>();
            //推送数据
            if (MethodEnum.POST.equalsIgnoreCase(method)) {
                results = post.process(bodyParam, this.url);
                logger.info("本次推送完成");
            } else if (MethodEnum.GET.equalsIgnoreCase(method)) {
//                    results = HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
            } else {
                logger.info("请求参数不合法，必须为post/get");
                throw DataXException.asDataXException(String.format("您填写的参数值不合法: [%s]"));
            }
            if (!"".equals(results)) {
                logger.info("发送请求成功，返回数据：", results);
            }
        }


        /**
         * 推送数据
         * @param records
         */
        private void handleDataIsNotKafka(List<Record> records) throws Exception {
            Post post = new Post();
            List<Map<String, Object>> list = new ArrayList<>();
            //数据格式转换处理
            for (Record record : records) {
                list.add(recordToJsonString(this.columnMap,record));
            }
            //记录推送数据的返回结果
            Map<String, String> results = new HashMap<>();
            //推送数据
            if (MethodEnum.POST.equalsIgnoreCase(method)) {
                results = post.process(list, this.url);
                logger.info("本次推送完成");
            } else if (MethodEnum.GET.equalsIgnoreCase(method)) {
//                    results = HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
            } else {
                logger.info("请求参数不合法，必须为post/get");
                throw DataXException.asDataXException(String.format("您填写的参数值不合法: [%s]"));
            }
            if (!"".equals(results)) {
                logger.info("发送请求成功，返回数据：{}", results);
            }


        }

        /**
         * 数据库查询数据
         * @param columnMap
         * @param record
         * @return
         */
        private HashMap<String, Object> recordToJsonString(List<Map> columnMap,Record record) {
            HashMap<String, Object> map = new HashMap<>(16);
            for (int i = 0; i < this.columnNumber; i++) {
                //获取columnName
                String columnName = columnMap.get(i).get(COLUMN_NAME).toString();
                //获取columnType
                String columnType = columnMap.get(i).get(COLUMN_TYPE).toString();
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
        private void putValueToMap(String columnName, String columnType, Column column, Map<String, Object> map) {
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
                case "Decimal":
                    map.put(columnName, column.asBigDecimal());
                    break;
                default:
                    throw DataXException.asDataXException("[column.columnType]有误：" + columnType);
            }
        }

        /**
         * @param record
         * @return
         */
        private Map<String, Object> recordToJsonString(Record record) {
                int recordLength = record.getColumnNumber();
                logger.info("record的recordLength：{}", recordLength);
                return JSONObject.parseObject(record.getColumn(0).asString());
        }
    }


}

package com.alibaba.datax.plugin.writer.hndlwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.alibaba.datax.plugin.writer.hndlwriter.RestFulApiWriterErrorCode.CONFIG_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.writer.hndlwriter.constant.CommonConstants.*;
import static com.alibaba.datax.plugin.writer.hndlwriter.constant.PushConstant.PUSH_CODE_SUCCESS;
import static com.alibaba.datax.plugin.writer.hndlwriter.constant.PushConstant.PUSH_MESSAGE_SUCCESS;
/**
 * @Author gxx
 * @Date 2023年02月17日15时36分
 */
public class HndlWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            logger.debug("job init begin...");
            this.originalConfig = super.getPluginJobConf();
            //TODO 参数校验
            validConfigParam();
        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> writerSplitConfigurations = new ArrayList<>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration writerSplitConfiguration = this.originalConfig.clone();
                writerSplitConfigurations.add(writerSplitConfiguration);
            }
            return writerSplitConfigurations;
            //由于此writer为http获取数据，暂时只单通道执行，如需拆分需指定参数计算后放入集合
//            List<Configuration> writerSplitConfiguration = new ArrayList<>();
//            writerSplitConfiguration.add(this.originalConfig);
//            return writerSplitConfiguration;
        }

        /**
         * 参数校验
         *
         * @return
         */
        private boolean validConfigParam() {
            Boolean flag = true;
            String url = this.originalConfig.getString(HttpConfigParam.URL);
            String method = this.originalConfig.getString(HttpConfigParam.METHOD);
            String topic = this.originalConfig.getString(HttpConfigParam.TOPIC);
            String bootstrapServers = this.originalConfig.getString(HttpConfigParam.BOOTSTRAP_SERVERS);
            List<String> aesColumnList = this.originalConfig.getList(HttpConfigParam.AES_PARAMS, String.class);
            List<Map> columnMapList = this.originalConfig.getList(HttpConfigParam.COLUMN_MAP, Map.class);
            List<String> pushColumnList = this.originalConfig.getList(HttpConfigParam.PUSH_COLUMNS, String.class);

            if (CollectionUtils.isEmpty(columnMapList) || CollectionUtils.isEmpty(aesColumnList) || CollectionUtils.isEmpty(pushColumnList)){
                flag = false;
            }
            if (StringUtils.isEmpty(method) || StringUtils.isEmpty(topic) || StringUtils.isEmpty(bootstrapServers) || StringUtils.isEmpty(url)){
                flag = false;
            }
            if (!flag){
                throw DataXException.asDataXException(CONFIG_INVALID_EXCEPTION, String.format("您的参数配置错误: [%s]", url, method,aesColumnList,topic,bootstrapServers,columnMapList));
            }
            List<String> columnList = new ArrayList<>();
            for (Map map : columnMapList) {
                columnList.add(map.get("columnName").toString());
            }
            for (String column : pushColumnList) {
                if (!columnList.contains(column)){
                    throw DataXException.asDataXException(CONFIG_INVALID_EXCEPTION, String.format("您的选择推送的参数配置错误: [%s]", column));
                }
            }
            return true;
        }

        /**
         * 正则匹配url地址2
         *
         * @param str 地址
         * @return
         */
        public static boolean isUrl(String str) {
            //转换为小写
            str = str.toLowerCase();
            String regex = new StringBuilder().append("^((https|http|ftp|rtsp|mms)?://)")
                    .append("?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?")
                    .append("(([0-9]{1,3}\\.){3}[0-9]{1,3}").append("|")
                    .append("([0-9a-z_!~*'()-]+\\.)*").append("([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\\.")
                    .append("[a-z]{2,6})").append("(:[0-9]{1,5})?").append("((/?)|")
                    .append("(/[0-9a-z_!~*'().;?:@&=+$,%#-]+)+/?)$").toString();
            return str.matches(regex);
        }


    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
        private Producer<String, String> producer;
        protected int batchSize = 2048;
        protected int batchByteSize = 32 * 1024 * 1024;
        protected String passTime;
        protected String url;
        protected String method;
        protected String dataPath;
        protected String topic;
        protected List<Map> columnMap;
        protected List<String> pushColumnList ;
        protected String payFile;
        protected List aesParamColumnList;
        @Override
        public void init() {
            logger.debug("task init begin...");
            this.writerSliceConfig = super.getPluginJobConf();
            //初始化kafka
            Properties props = new Properties();
            props.put("bootstrap.servers", writerSliceConfig.getString(HttpConfigParam.BOOTSTRAP_SERVERS));
            props.put("acks", "all");//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强地保证。
            props.put("retries", 0);
            // Controls how much bytes sender would wait to batch up before publishing to Kafka.
            //控制发送者在发布到kafka之前等待批处理的字节数。
            //控制发送者在发布到kafka之前等待批处理的字节数。 满足batch.size和ling.ms之一，producer便开始发送消息
            //默认16384   16kb
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver, RecordSender recordSender) throws Exception {
            String dateFormat = DATEFORMAT;
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            this.passTime = sdf.format(new Date());
            this.url = this.writerSliceConfig.getString(HttpConfigParam.URL);
            this.method = this.writerSliceConfig.getString(HttpConfigParam.METHOD);
            this.dataPath = this.writerSliceConfig.getString(HttpConfigParam.DATA_PATH);
            this.topic = this.writerSliceConfig.getString(HttpConfigParam.TOPIC);
            this.columnMap = this.writerSliceConfig.getList(HttpConfigParam.COLUMN_MAP,Map.class);
            this.pushColumnList = this.writerSliceConfig.getList(HttpConfigParam.PUSH_COLUMNS,String.class);
            this.payFile = this.writerSliceConfig.getString(HttpConfigParam.PAY_FILE_UPLOAD);
            logger.info("payfile::",payFile);
            Map<String,String> results = new HashMap<>(16);

           //获取需要加密的推送参数字段
            this.aesParamColumnList = this.writerSliceConfig.getList(HttpConfigParam.AES_PARAMS, String.class);
            //获取datax的数据
            List<Record> pushDataBuffer = new ArrayList<>();
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    bufferBytes += record.getMemorySize();
                        pushDataBuffer.add(record);
                    if (pushDataBuffer.size() >= batchSize || bufferBytes >= batchByteSize ) {

                        if (pushDataBuffer != null && !pushDataBuffer.isEmpty()) {
                            for (Record buffer : pushDataBuffer) {
                                handleData(buffer);
                            }
                        }
                        pushDataBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                
                if (pushDataBuffer != null && !pushDataBuffer.isEmpty()) {
                    for (Record buffer : pushDataBuffer) {
                        handleData(buffer);
                    }
                    pushDataBuffer.clear();
                }
                bufferBytes = 0;

            } finally {
                pushDataBuffer.clear();
                bufferBytes = 0;
            }
            }

        private void handleData(Record record) throws Exception {
            String bodyParam = recordToJsonString(columnMap,record);
            //材料接口分支
            if (this.payFile != "" && PAY_FILE.equals(this.payFile)){
                String fileStr = "khhBBCiFFRgBJCCCGEEKOiACWEEEIIIUZFAUoIIYQQQoyKApQQQgghhBgVBSghhBBCCDEi4P8HvNp/KdM1m+gAAAAASUVORK5CYII=";
                String reportCode = "KTH4118080215055";
                String fileType = "1";
                String fileName = "微信图片_20230306161512.png";
                String fileSize = "65536Byte";

                Map<String,Object> map = new HashMap<>(16);
                map.put("reportCode",reportCode);
                map.put("fileType",fileType);
                map.put("fileName",fileName);
                map.put("fileSize",fileSize);
                map.put("fileStr",fileStr);
                String param = JSON.toJSONString(map);
            }else {
                Map<String,String> bodyParamMap = filterColumn(bodyParam,pushColumnList);
                //组装请求体的数据
                //自定义数据加密
                AES aes = new AES();
                Map<String, Object> body = aes.aes(AES_KEY,JSON.toJSONString(bodyParamMap),this.aesParamColumnList);
                String bodyStr = JSONObject.toJSONString(body);
                logger.info("加密后的数据：{}",bodyStr);
                //query请求参数
                Map<String, Object> queryParam = this.writerSliceConfig.getMap(HttpConfigParam.QUERY_PARAM, Object.class);
                //获取请求头信息
                Map<String, Object> headerInfosMap = this.writerSliceConfig.getMap(HttpConfigParam.HEADER_INFOS, Object.class);
                Map<String,String> results = new HashMap<>();
                //推送数据
                if (MethodEnum.POST.equalsIgnoreCase(method)) {
                    results = HttpServiceUtil.insureResponsePost(url, bodyStr);
                    //请求完成后，记录推送信息
                    String code = results.get(CODE);
                    String message = results.get(MESSAGE);
                    kafkaWriter(columnMap,topic,record,passTime,PUSH_CODE_SUCCESS,PUSH_MESSAGE_SUCCESS,url,code,message);
                } else if (MethodEnum.GET.equalsIgnoreCase(method)) {
//                    results = HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
                } else {
                    logger.info("请求参数不合法，必须为post/get");
                    throw DataXException.asDataXException(RestFulApiWriterErrorCode.ILLEGAL_VALUE, String.format("您填写的参数值不合法: [%s]"));
                }
                if ( !"".equals(results)){
                    logger.info("发送请求成功，返回数据：",results);
                }
        }
        }




        private Map<String,String> filterColumn(String body, List<String> pushColumnList){
            Map<String,String> paramMap = new HashMap<>(16);
            Map<String,String> bodyMap = JSON.parseObject(body, Map.class);
            Map<String,String> defaultDataMap = new HashMap<>(16);
            logger.info("topic:{}",topic);
            switch (this.topic){
                case "RH_sjhj_hndllp_PushRecord_LiPei":
                    defaultDataMap.put("reportCode","0");
                    defaultDataMap.put("payCaseCode","0");
                    defaultDataMap.put("endCaseCode","0");
                    defaultDataMap.put("caseState","无");
                    defaultDataMap.put("fileUploadDate","1997-01-01 00:00:00");
                    defaultDataMap.put("endCaseDate","1997-01-01 00:00:00");
                    defaultDataMap.put("payType","无");
                    defaultDataMap.put("damageLevel","无");
                    defaultDataMap.put("endpayAmount","0.00");
                    defaultDataMap.put("prepaidAmount","0.00");
                    defaultDataMap.put("actualAmount","0.00");
                    defaultDataMap.put("insuretypeNames","无");
                    break;
                case "RH_sjhj_hndllp_PushRecord_BaoAn":
                    defaultDataMap.put("reportCode","0");
                    defaultDataMap.put("policyCode","0");
                    defaultDataMap.put("holderName","无");
                    defaultDataMap.put("caseDate","1997-01-01 00:00:00");
                    defaultDataMap.put("reportDate","1997-01-01 00:00:00");
                    defaultDataMap.put("casePlace","无");
                    defaultDataMap.put("caseCauseName","无");
                    defaultDataMap.put("reportPerson","无");
                    defaultDataMap.put("reportPhone","无");
                    defaultDataMap.put("damagedItem","无");
                    defaultDataMap.put("estimateAmount","0.00");
                    defaultDataMap.put("caseType","无");
                    defaultDataMap.put("insuretypeName","无");
                    defaultDataMap.put("personName","无");
                    defaultDataMap.put("personCode","无");
                    defaultDataMap.put("hospital","无");
                    break;
                case "RH_sjhj_hndllp_PushRecord_ZhiFu":
                    defaultDataMap.put("reportCode","0");
                    defaultDataMap.put("paymentSuccess","无");
                    defaultDataMap.put("paymentTime","1997-01-01 00:00:00");
                    defaultDataMap.put("paymentObject","无");
                    defaultDataMap.put("paymentBankcard","6211112223333333334");
                    defaultDataMap.put("paymentAmount","0.00");
                    break;
                default:
                    break;
            }
            logger.info("默认map：{}",defaultDataMap);
            logger.info("bodyMap:{}",bodyMap);
            logger.info("push字段：{}",pushColumnList);
            for (String key : pushColumnList) {
                if (bodyMap.containsKey(key)){
                    paramMap.put(key,bodyMap.get(key));
                }else {
                    paramMap.put(key,defaultDataMap.get(key));
                }
            }
            return paramMap;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        private void kafkaWriter(List<Map> columnMap,String topic, Record record, String passTime, String pushCode, String pushMessage, String pushUrl, String code, String message){
            String value = recordToJsonString(columnMap,record);
            Map<String, Object> valueMap = JSONObject.parseObject(value);
            //数据推送时间
            valueMap.put(PASS_TIME, passTime);
            //数据推送状态
            valueMap.put(PUSH_CODE, pushCode);
            //数据推送信息
            valueMap.put(PUSH_MESSAGE, pushMessage);
            //数据推送接口
            valueMap.put(PUSH_URL, pushUrl);
            //数据推送返回状态码
            valueMap.put(RESPONSE_CODE, code);
            //数据推送返回信息
            valueMap.put(RESPONSE_MESSAGE, message);
            producer.send(new ProducerRecord<>(topic, JSONObject.toJSONString(valueMap)));
        }

        private String recordToJsonString(List<Map> columnMap,Record record) {
            int recordLength = record.getColumnNumber();
            logger.info("record的recordLength：{}",recordLength);
            HashMap<String, Object> map = new HashMap<>(16);
            for (int i = 0; i < recordLength; i++) {
                //获取columnName
                String columnName = columnMap.get(i).get(COLUMN_NAME).toString();
                //获取columnType
                String columnType = columnMap.get(i).get(COLUMN_TYPE).toString();
                //获取column
                Column column = record.getColumn(i);
                putValueToMap(columnName, columnType, column, map);
            }
            return JSON.toJSONString(map);
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
                    map.put(columnName, column.asString());
                    break;
                case "Double":
                    map.put(columnName, column.asDouble());
                    break;
                case "Decimal":
                    map.put(columnName, column.asBigDecimal());
                    break;
                default:
                    throw DataXException.asDataXException(KafkaWriterErrorCode.KAFKA_WRITER_ERROR,
                            "[column.columnType]有误：" + columnType);
            }
        }
    }


}

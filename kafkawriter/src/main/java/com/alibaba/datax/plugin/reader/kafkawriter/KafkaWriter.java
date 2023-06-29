package com.alibaba.datax.plugin.reader.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @author yuejinfu
 */
public class KafkaWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
            log.info("kafka writer params:{}", conf.toJSON());
            //校验 参数配置
            this.validateParameter();
        }

        private void validateParameter() {
            //toipc 必须填
            this.conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.BOOTSTRAP_SERVERS, KafkaWriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            //按照reader 配置文件的格式  来 组织相同个数的writer配置文件
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }


        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }


    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Producer<String, String> producer;

        private Configuration conf;

        private List<Map> columnMap;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            columnMap = conf.getList(Key.COLUMN, Map.class);

            System.out.println("--------------column: \n" + columnMap + "\n\n");

            //初始化kafka
            Properties props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.BOOTSTRAP_SERVERS));
            props.put("acks", "all");//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
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
        public void startWrite(RecordReceiver recordReceiver, RecordSender recordSender) {

            log.info("start to writer kafka");
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {//说明还在读取数据,或者读取的数据没处理完
                //获取一行数据，按照指定分隔符 拼成字符串 发送出去
                producer.send(new ProducerRecord<>(this.conf.getString(Key.TOPIC), recordToJsonString(record)));
            }

//            record = recordReceiver.getFromReader();
//            producer.send(new ProducerRecord<>(this.conf.getString(Key.TOPIC), recordToJsonString(record)));
        }

        @Override
        public void destroy() {
            if (producer != null) {
                producer.close();
            }
        }

        private String recordToJsonString(Record record) {
            int recordLength = record.getColumnNumber();
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < recordLength; i++) {

//                System.out.println("--------------column-" + i + "  :" + columnMap.get(i));

                //获取columnName
                String columnName = columnMap.get(i).get("columnName").toString();
                //获取columnType
                String columnType = columnMap.get(i).get("columnType").toString();
                //获取column
                Column column = record.getColumn(i);

                putValueToMap(columnName, columnType, column, map);
            }
            return JSONObject.toJSONString(map);
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
                    map.put(columnName, column.getRawData());
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
package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.fastjson2.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * @author yuejinfu
 */
public class KafkaReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // warn: 忽略大小写
            String topic = this.originalConfig.getString(Key.TOPIC);
            Integer partitions = this.originalConfig.getInt(Key.KAFKA_PARTITIONS);
            String bootstrapServers = this.originalConfig.getString(Key.BOOTSTRAP_SERVERS);
            String groupId = this.originalConfig.getString(Key.GROUP_ID);
            Integer isList = this.originalConfig.getInt(Key.IS_LIST);
            List column = this.originalConfig.getList(Key.COLUMN);
            String autoOffsetReset = this.originalConfig.getString(Key.AUTO_OFFSET_RESET);

            if (null == topic) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.TOPIC_ERROR, "没有设置参数[topic]");
            }
            if (partitions == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARTITION_ERROR, "没有设置参数[kafka.partitions]");
            } else if (partitions < 1) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARTITION_ERROR, "[kafka.partitions]不能小于1");
            }
            if (null == bootstrapServers) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.ADDRESS_ERROR, "没有设置参数[bootstrap.servers]");
            }
            if (isList == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "没有设置参数[kafka.isList]");
            } else if (isList > 1 || isList < 0) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "[kafka.isList]取值有误，1：列表类型 0：非列表类型");
            }
            if (null == groupId) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "没有设置参数[groupid]");
            }
            if (column == null || column.size() == 0) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "没有设置[column]参数");
            }
            if (autoOffsetReset == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "没有设置[autoOffsetReset]参数,默认：latest");
            }
        }

        @Override
        public void preCheck() {
            init();

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();

            Integer partitions = this.originalConfig.getInt(Key.KAFKA_PARTITIONS);
            for (int i = 0; i < partitions; i++) {
                configurations.add(this.originalConfig.clone());
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

    public static class Task extends Reader.Task {

        private static final Logger logger = LoggerFactory.getLogger(CommonRdbmsReader.Task.class);
        //配置文件
        private Configuration readerSliceConfig;
        //是否停止拉取数据
        private boolean flag;
        //kafka address
        private String bootstrapServers;
        //kafka groupid
        private String groupId;
        //kafkatopic
        private String kafkaTopic;
        //是否为列表类型 1：是 0：非
        private Integer isList;
        //kafkareader端的每个关键字的key
        private List column;
        private String autoOffsetReset;


        @Override
        public void init() {
            flag = true;
            this.readerSliceConfig = super.getPluginJobConf();
            bootstrapServers = this.readerSliceConfig.getString(Key.BOOTSTRAP_SERVERS);
            groupId = this.readerSliceConfig.getString(Key.GROUP_ID);
            kafkaTopic = this.readerSliceConfig.getString(Key.TOPIC);
            column = this.readerSliceConfig.getList(Key.COLUMN);
            isList = this.readerSliceConfig.getInt(Key.IS_LIST);
            autoOffsetReset = this.readerSliceConfig.getString(Key.AUTO_OFFSET_RESET);
        }

        @Override
        public void startRead(RecordSender recordSender) {


            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null ? groupId : UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset != null ? autoOffsetReset : "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "50000");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

//            Properties props = new Properties();
//            props.put("bootstrap.servers", bootstrapServers);
//            props.put("group.id", groupId != null ? groupId : UUID.randomUUID().toString());
//            props.put("auto.offset.reset", autoOffsetReset != null ? autoOffsetReset : "latest");
//            props.put("enable.auto.commit", "false");
//            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            try {

                consumer.subscribe(Collections.singletonList(kafkaTopic));
                while (flag) {
                    logger.info("开始从kafka拉取数据-----> bootstrapServers:" + bootstrapServers + "   groupId:" + groupId + " kafkaTopic:" + kafkaTopic + " autoOffsetReset:" + autoOffsetReset);
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    logger.info("records.count ---> " + records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("进入了循环--->");
                        String value = record.value();
                        logger.info("从kafka拉取到的value-----> " + value);
                        //构建record以及column
                        buildOneRecord(recordSender, value);
                    }
                    consumer.commitSync();

                    //判断数据是否读取完毕
                    if (records.isEmpty()) {
                        logger.info("读取完毕，轮询结束！");
                        destroy();
                    }
                }

            } finally {
                consumer.close();
            }
        }


        private void buildOneRecord(RecordSender recordSender, String value) {
            List<Map> maps = new ArrayList<>();

            if (isList == 1) {
                maps = JSON.parseArray(value, Map.class);
            } else if (isList == 0) {
                maps.add(JSON.parseObject(value, Map.class));
            } else {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "[kafka.isList]参数设置有误");
            }

            for (Map map : maps) {
                logger.info("maps--->map--->" + map.toString());
                Record record = recordSender.createRecord();
                for (Object column : column) {
                    String columnStr;
                    try {
                        Object columnVal = map.get(column.toString());
                        if (null != columnVal) {
                            columnStr = columnVal.toString();
                            record.addColumn(new StringColumn(columnStr));
                        } else {
//                            record.addColumn(null);
                            record.addColumn(new StringColumn(null));
                        }

                    } catch (NullPointerException e) {
                        throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR, "[kafka.column]数据中没有该column字段：" + column.toString());
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            flag = false;
        }


//        public static void main(String[] args) {
//            Properties props = new Properties();
//
//            props.put("bootstrap.servers", "192.168.10.67:9092");
//            props.put("group.id", "test-consumer-group");
//            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("enable.auto.commit", "false");
//            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//            consumer.subscribe(Collections.singletonList("first"));
//            boolean flag = true;
//            while (flag) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//                System.out.println(records.count());
//
//                System.out.println("---------------------isempty>" + records.isEmpty());
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("------------------------------->" + record.value());
//                }
//                consumer.commitSync();
//                if (records.isEmpty()) {
//                    flag = false;
//                }
//            }
//        }


//        public static void main(String[] args) {
//
//            Properties props = new Properties();
//            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.11.106:9092,10.1.11.107:9092,10.1.11.108:9092");
//            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
//            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
//            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "50000");
//            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
//            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//
//            List<String> topics = Arrays.asList(new String[]{"TestTopic_AAA1111"});
//
//            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//            consumer.subscribe(topics);
//
//            while (true) {
//                try {
//                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//                    System.out.println("---------------------isEmpty>" + records.isEmpty());
//                    for (ConsumerRecord<String, String> record : records) {
//                        try {
//                            System.out.println("------------------------------->" + record.value());
//                        } catch (Exception e) {
//                            logger.error("", e);
//                        }
//                    }
//
//                    System.out.println("---------------------等待");
//                    Thread.sleep(1000 * 5);
//                } catch (Exception e) {
//                    logger.error("", e);
//                }
//
//            }
//        }


    }
}

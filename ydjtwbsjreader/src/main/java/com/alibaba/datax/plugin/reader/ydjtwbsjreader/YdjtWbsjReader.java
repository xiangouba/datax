package com.alibaba.datax.plugin.reader.ydjtwbsjreader;

import ch.ethz.ssh2.crypto.cipher.DES;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.gyljr.xscyl.encryptutil.ExceptionUtil.ServiceException;
import com.gyljr.xscyl.encryptutil.SM4Util.SM4Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.*;
/**
 * @author gxx
 * @date 2023/04/11 10:49
 */
public class YdjtWbsjReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            logger.debug("job init begin...");
            this.originalConfig = super.getPluginJobConf();
            //TODO 参数校验
            validConfigParam();
        }

        //todo 参数校验
        private boolean validConfigParam(){
            String url = this.originalConfig.getString(HttpConfigParam.URL);
            String method = this.originalConfig.getString(HttpConfigParam.METHOD);
            List<String> list = this.originalConfig.getList(HttpConfigParam.COLUMN, String.class);
            if (StringUtils.isNotBlank(method) && list.size()>0){
                return true;
            } else {
                logger.info("请求参数校验URL: " + url);
                logger.info("请求参数校验METHOD: " + method);
                logger.info("请求参数校验COLUMN: " + list.toString());
                throw DataXException.asDataXException(RestFulApiReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("您的参数配置错误: [%s]", url, method, list));
            }
        }

        //todo 正则匹配url地址2
        public static boolean isUrl(String str){
            //转换为小写
            str = str.toLowerCase();
            String regex = new StringBuilder().append("^((https|http|ftp|rtsp|mms)?://)")
                    .append("?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?")
                    .append("(([0-9]{1,3}\\.){3}[0-9]{1,3}").append("|")
                    .append("([0-9a-z_!~*'()-]+\\.)*").append("([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\\.")
                    .append("[a-z]{2,6})").append("(:[0-9]{1,5})?").append("((/?)|")
                    .append("(/[0-9a-z_!~*'().;?:@&=+$,%#-]+)+/?)$").toString();
            return  str.matches(regex);
        }

        @Override
        public void prepare(){

        }
        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfiguration = new ArrayList<>();
            readerSplitConfiguration.add(this.originalConfig);
            return readerSplitConfiguration;
        }

    }

    public static class Task extends Reader.Task {
        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration readerSliceConfig;
        private String[] array;
        private String dataPath;
        private List<String> columnList;
        //公司名字
        private List<String> companyNameList;

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
        private String autoOffsetReset;


        @Override
        public void init() {
            flag = true;
            this.readerSliceConfig = super.getPluginJobConf();
            bootstrapServers = this.readerSliceConfig.getString(Key.BOOTSTRAP_SERVERS);
            groupId = this.readerSliceConfig.getString(Key.GROUP_ID);
            kafkaTopic = this.readerSliceConfig.getString(Key.TOPIC);
            isList = this.readerSliceConfig.getInt(Key.IS_LIST);
            autoOffsetReset = this.readerSliceConfig.getString(Key.AUTO_OFFSET_RESET);
            logger.debug("task init begin...");
        }

        /**
         * 判断字符串是否可以转化为json对象
         * @param content
         * @return
         */
        public static boolean isJsonObject(String content) {
            // 此处应该注意，不要使用StringUtils.isEmpty(),因为当content为"  "空格字符串时，JSONObject.parseObject可以解析成功，
            // 实际上，这是没有什么意义的。所以content应该是非空白字符串且不为空，判断是否是JSON数组也是相同的情况。
            if(StringUtils.isBlank(content)) {
                return false;
            }
            try {
                JSONObject jsonStr = JSONObject.parseObject(content);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        /**
         * 判断字符串是否可以转化为JSON数组
         * @param content
         * @return
         */
        public static boolean isJsonArray(String content) {
            if(StringUtils.isBlank(content)) {
                return false;
            }
            StringUtils.isEmpty(content);
            try {
                JSONArray jsonStr = JSONArray.parseArray(content);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            //公司参数集合
            this.companyNameList = this.readerSliceConfig.getList(HttpConfigParam.COMPANY_NAME,String.class);
            //根据执行不同的同步策略
            if (CollectionUtils.isEmpty(this.companyNameList)){
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
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                try {
                    consumer.subscribe(Collections.singletonList(kafkaTopic));
                    while (flag) {
                        logger.info("开始从kafka拉取数据-----> bootstrapServers:" + bootstrapServers + "   groupId:" + groupId + " kafkaTopic:" + kafkaTopic + " autoOffsetReset:" + autoOffsetReset);
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                        logger.info("records.count ---> " + records.count());
                        for (ConsumerRecord<String, String> record : records) {
                            logger.info("进入了循环--->");
                            String value = record.value();
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            logger.info("从kafka拉取到的value-----> " + value);
                            //将kafka读取到的值放入城市参数集合中
                            this.companyNameList.add((String)jsonObject.get("companyName"));
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
            //获取字段和字段顺序
            this.columnList = this.readerSliceConfig.getList(HttpConfigParam.COLUMN, String.class);
            logger.info("column字段：  " + columnList.toString());
            //List<Object>转String[]有可能报错！！！
            array = (String[]) columnList.toArray(new String[columnList.size()]);
            //从BODY体请求参数
            String bodyParam = this.readerSliceConfig.getString(HttpConfigParam.BODY_PARAM);
            //query请求参数
            Map<String, Object> queryParam = this.readerSliceConfig.getMap(HttpConfigParam.QUERY_PARAM, Object.class);
            String url = this.readerSliceConfig.getString(HttpConfigParam.URL);
            String method = this.readerSliceConfig.getString(HttpConfigParam.METHOD);
            this.dataPath =  this.readerSliceConfig.getString(HttpConfigParam.DATA_PATH);
            //请求参数处理
            Map<String,Object> objectMap = JSONObject.parseObject(bodyParam);
            //获取请求头信息
            Map<String, Object> headerInfosMap = this.readerSliceConfig.getMap(HttpConfigParam.HEADER_INFOS,
                    Object.class);
            //出参
            String result;
            //入参
            String requestJson;
            //入参加密
            String key = "JFF8U9wIpOMfs2Y3";
            String id = "2cf1ae4a502d4b128ee7e649ac3473af";
            if (CollectionUtils.isEmpty(this.companyNameList)){
                return;
            }
            for (String companyName : this.companyNameList) {
                objectMap.put("companyName",companyName);
                try {
                    requestJson = CipherText.generateSMRequestJson(objectMap,key,id);
                } catch (ServiceException e) {
                    throw new RuntimeException("加密失败");
                }
                if (MethodEnum.POST.equalsIgnoreCase(method)){
                    result = HttpServiceUtil.insureResponsePost(url,requestJson);
//                    Map<String,Object> resultMap = JSONObject.parseObject(result);
                    logger.info("请求返回的数据：{}",result);
                    //解密
                    String decryptData = null;
                    try {
                        logger.info("请求后的获取出来的data：{}",JSONObject.parseObject(result).get("data"));
                        decryptData = SM4Util.decryptData(JSONObject.parseObject(result).get("data").toString(), key);
                        logger.info("解密后的数据：{}", decryptData);
                    } catch (ServiceException e) {
                        throw new RuntimeException("解密失败");
                    }
                    JSONObject dataObject = JSONObject.parseObject(decryptData);
                    if (!CollectionUtils.isEmpty(dataObject) && (int)dataObject.get("code") == 200){
                        postQueryData(dataObject.get("data").toString(), recordSender);
                    }else {
                        logger.info("POST获取数据失败");
                        throw DataXException.asDataXException(RestFulApiReaderErrorCode.DATA_QUERY_FAILED,
                                String.format("获取数据失败: [%s]"));
                    }
                }else if (MethodEnum.GET.equalsIgnoreCase(method)){
                    result = HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
                    if (StringUtils.isNotBlank(result)){
                        postQueryData(result, recordSender);
                    }else {
                        logger.info("GET获取数据失败");
                        throw DataXException.asDataXException(RestFulApiReaderErrorCode.DATA_QUERY_FAILED,
                                String.format("获取数据失败: [%s]"));
                    }
                }else {
                    logger.info("请求参数不合法，必须为post/get");
                    throw DataXException.asDataXException(RestFulApiReaderErrorCode.ILLEGAL_VALUE,
                            String.format("您填写的参数值不合法: [%s]"));
                }
            }
        }

        //dataPath分隔符
        private static final String SPLIT_POINT = ".";
        //json匹配
        private static final String JSON_START_WITH = "[{";

        //todo 保证数据都能JSON格式化
        private void postQueryData(String result, RecordSender recordSender){
            logger.info("dataPath:  " + this.dataPath);
            try {
                //list集合数据直接遍历结果集，是jsonArray
                if (isJsonArray(result)){
                    execIterator(result, recordSender);
                    logger.info("1");
                }else //json对象
                logger.info("2");
                    if (isJsonObject(result)){
                    //如果dataPath是多层路径且是以点分隔
                    if (dataPath.contains(SPLIT_POINT)) {
                        logger.info("3");
                        //split分割字符串需转义(Windows中双反斜杠，Linux中单反斜杠)
                        String[] split = dataPath.split("\\.");
                        String lastStr = split[split.length - 1];
                        JSONObject jsonObject = JSON.parseObject(result);
                        logger.info("dataPath取值按照英文小数点分割后的最后一层路径：  "+lastStr);
                        //递归根据dataPath的最后一层路径取数据
                        String res = JsonLoop.jsonLoop(jsonObject, lastStr);
                        //数据长度大于0
                        if (res.length() > 0) {
                            if (res.startsWith(JSON_START_WITH)) {
                                //数据是以[{开头，走结果遍历
                                execIterator(res, recordSender);
                            } else {
                                //数据是单个json对象
                                execJsonObject(res, recordSender);
                            }
                        }else {
                            logger.info("未根据dataPath获取到数据");
                            throw DataXException.asDataXException(RestFulApiReaderErrorCode.IS_NULL,
                                    String.format("空值出现异常, 未根据dataPath获取到数据: [%s]"));
                        }
                    }else {
                        //数据是单个json对象
                        execJsonObject(result, recordSender);
                    }

                }
            } catch (Exception e){
                logger.info("数据转换失败或不支持此类型",e);
                throw DataXException.asDataXException(RestFulApiReaderErrorCode.DATA_CONVERSION_FAILED,
                        String.format("数据转换失败或不支持此类型: [%s]"));
            }
        }

        //todo 遍历结果集
        private void execIterator(String result, RecordSender recordSender){
            JSONArray jsonArray = JSON.parseArray(result);
            for (int i = 0; i < jsonArray.size(); i++) {
                Map map = (Map) jsonArray.get(i);
                Record record = recordSender.createRecord();
                logger.info("A");
                coumplute(recordSender, map, record);
            }
        }

        //todo 单个json对象
        private void execJsonObject(String result, RecordSender recordSender){
            logger.info("转换数据：[]",result);
            JSONObject object = JSON.parseObject(result);
            Map<String, Object> map = new HashMap<>(16);
            Iterator<Map.Entry<String, Object>> iterator = object.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                map.put(entry.getKey(), entry.getValue());
            }
            Record record = recordSender.createRecord();
            logger.info("map的结构：[]",map);
            coumplute(recordSender, map, record);
        }

        //todo 数据按照字段顺序排列匹配数据库字段
        private void coumplute(RecordSender recordSender, Map<String, Object> map, Record record){
            List<Map<String,Object>> data = new ArrayList<>();
            try {

                logger.info("数据按照字段顺序排列匹配数据库字段：[]",map);
//                if (200 != (int) map.get("code")){
//                    throw DataXException.asDataXException(RestFulApiReaderErrorCode.DATA_CODE_FAILED,
//                            String.format("接口返回code错误: [%s]",map.get("code")));
//                }
                logger.info("BBBB");
                String dataStr = (String) map.get(this.dataPath);

                LinkedList<Object> linkedList = new LinkedList<>();
                for (String column : this.columnList) {
                    linkedList.add(map.get(column));
                }
                linkedList.forEach(val -> {
                    if (val instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) val));
                    } else if (val instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) val));
                    } else if (val instanceof Date) {
                        record.addColumn(new DateColumn((Date) val));
                    } else if (val instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) val));
                    }else if (val instanceof Long) {
                        record.addColumn(new LongColumn((Long) val));
                    }else {
                        record.addColumn(new StringColumn((String) val));
                    }
                });
                //System.out.println("record===============" + record);
                recordSender.sendToWriter(record);
            }catch (Exception e){
                throw DataXException.asDataXException(RestFulApiReaderErrorCode.DATA_CONVERSION_FAILED,
                        String.format("数据转换失败或不支持此类型: [%s]"));
            }
        }
    }

}

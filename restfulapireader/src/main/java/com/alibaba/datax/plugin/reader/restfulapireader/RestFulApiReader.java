package com.alibaba.datax.plugin.reader.restfulapireader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
/**
 * @author LF
 * @date 2020/12/17 - 10:01
 */
public class RestFulApiReader extends Reader {

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
            /*List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration readerSplitConfiguration = this.originalConfig.clone();
                readerSplitConfigurations.add(readerSplitConfiguration);
            }
            return readerSplitConfigurations;*/
            //由于此reader为http获取数据，暂时只单通道执行，如需拆分需指定参数计算后放入集合
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

        @Override
        public void init() {
            logger.debug("task init begin...");
            this.readerSliceConfig = super.getPluginJobConf();
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

            //获取字段和字段顺序
            List column = this.readerSliceConfig.getList(com.alibaba.datax.plugin.reader.restfulapireader.HttpConfigParam.COLUMN, String.class);
            logger.info("column字段：  " + column.toString());
            //List<Object>转String[]有可能报错！！！
            array = (String[]) column.toArray(new String[column.size()]);
            //从BODY体请求参数
            String bodyParam = this.readerSliceConfig.getString(HttpConfigParam.BODY_PARAM);
            //query请求参数
            Map<String, Object> queryParam = this.readerSliceConfig.getMap(HttpConfigParam.QUERY_PARAM, Object.class);
            String url = this.readerSliceConfig.getString(HttpConfigParam.URL);
            String method = this.readerSliceConfig.getString(HttpConfigParam.METHOD);
            this.dataPath =  this.readerSliceConfig.getString(HttpConfigParam.DATA_PATH);
            //获取请求头信息
            Map<String, Object> headerInfosMap = this.readerSliceConfig.getMap(HttpConfigParam.HEADER_INFOS,
                    Object.class);
            String result;
                if (MethodEnum.POST.equalsIgnoreCase(method)){
                    result = HttpServiceUtil.insureResponsePost(url, bodyParam, headerInfosMap, queryParam);
                    if (StringUtils.isNotBlank(result)){
                        postQueryData(result, recordSender);
                    }else {
                        logger.info("POST获取数据失败");
                        throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.DATA_QUERY_FAILED,
                                String.format("获取数据失败: [%s]"));
                    }
                }else if (com.alibaba.datax.plugin.reader.restfulapireader.MethodEnum.GET.equalsIgnoreCase(method)){
                    result = com.alibaba.datax.plugin.reader.restfulapireader.HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
                    if (StringUtils.isNotBlank(result)){
                        postQueryData(result, recordSender);
                    }else {
                        logger.info("GET获取数据失败");
                        throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.DATA_QUERY_FAILED,
                                String.format("获取数据失败: [%s]"));
                    }
                }else {
                    logger.info("请求参数不合法，必须为post/get");
                    throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.ILLEGAL_VALUE,
                            String.format("您填写的参数值不合法: [%s]"));
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
                }else //json对象
                    if (isJsonObject(result)){
                    //如果dataPath是多层路径且是以点分隔
                    if (dataPath.contains(SPLIT_POINT)) {
                        //split分割字符串需转义(Windows中双反斜杠，Linux中单反斜杠)
                        String[] split = dataPath.split("\\.");
                        String lastStr = split[split.length - 1];
                        JSONObject jsonObject = JSON.parseObject(result);
                        logger.info("dataPath取值按照英文小数点分割后的最后一层路径：  "+lastStr);
                        //递归根据dataPath的最后一层路径取数据
                        String res = com.alibaba.datax.plugin.reader.restfulapireader.JsonLoop.jsonLoop(jsonObject, lastStr);
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
                            throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.IS_NULL,
                                    String.format("空值出现异常, 未根据dataPath获取到数据: [%s]"));
                        }
                    }else {
                        //数据是单个json对象
                        execJsonObject(result, recordSender);
                    }

                }
            } catch (Exception e){
                logger.info("数据转换失败或不支持此类型");
                throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.DATA_CONVERSION_FAILED,
                        String.format("数据转换失败或不支持此类型: [%s]"));
            }
        }

        //todo 遍历结果集
        private void execIterator(String result, RecordSender recordSender){
            JSONArray jsonArray = JSON.parseArray(result);
            for (int i = 0; i < jsonArray.size(); i++) {
                Map map = (Map) jsonArray.get(i);
                Record record = recordSender.createRecord();
                coumplute(recordSender, map, record);
            }
        }

        //todo 单个json对象
        private void execJsonObject(String result, RecordSender recordSender){
            JSONObject object = JSON.parseObject(result);
            Map<String, Object> map = new HashMap<>(16);
            Iterator<Map.Entry<String, Object>> iterator = object.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                map.put(entry.getKey(), entry.getValue());
            }
            Record record = recordSender.createRecord();
            coumplute(recordSender, map, record);
        }

        //todo 数据按照字段顺序排列匹配数据库字段
        private void coumplute(RecordSender recordSender, Map<String, Object> map, Record record){
            try {
                LinkedList<Object> linkedList = new LinkedList<>();
                for (int i = 0; i < array.length; i++) {

                    linkedList.add(map.get(array[i]));
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
                throw DataXException.asDataXException(com.alibaba.datax.plugin.reader.restfulapireader.RestFulApiReaderErrorCode.DATA_CONVERSION_FAILED,
                        String.format("数据转换失败或不支持此类型: [%s]"));
            }
        }
    }

}

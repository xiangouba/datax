package com.alibaba.datax.plugin.writer.restfulapiwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;

import java.util.*;

/**
 * @author LF
 * @date 2020/12/17 - 10:01
 */
public class RestFulApiWriter extends Writer {

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
            /*List<Configuration> writerSplitConfigurations = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration writerSplitConfiguration = this.originalConfig.clone();
                writerSplitConfigurations.add(writerSplitConfiguration);
            }
            return writerSplitConfigurations;*/
            //由于此writer为http获取数据，暂时只单通道执行，如需拆分需指定参数计算后放入集合
            List<Configuration> writerSplitConfiguration = new ArrayList<>();
            writerSplitConfiguration.add(this.originalConfig);
            return writerSplitConfiguration;
        }

        /**
         * 参数校验
         *
         * @return
         */
        private boolean validConfigParam() {
            String url = this.originalConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.URL);
            String method = this.originalConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.METHOD);
            List<String> columnList = this.originalConfig.getList(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.COLUMN, String.class);
            if (isUrl(url) && StringUtils.isNotBlank(method) && columnList.size() > 0) {
                return true;
            } else {
                logger.info("请求参数校验URL: " + url);
                logger.info("请求参数校验METHOD: " + method);
                logger.info("请求参数校验COLUMN: " + columnList.toString());
                throw DataXException.asDataXException(com.alibaba.datax.plugin.writer.restfulapiwriter.RestFulApiWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                        String.format("您的参数配置错误: [%s]", url, method, columnList));
            }
        }

        /**
         * 正则匹配url地址2
         *
         * @param str
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
        private String[] array;

        //dataPath分隔符
        private static final String SPLIT_POINT = ".";
        //json匹配
        private static final String JSON_START_WITH = "[{";

        @Override
        public void init() {
            logger.debug("task init begin...");
            this.writerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {

            //获取字段和字段顺序
            List columnList = this.writerSliceConfig.getList(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.COLUMN, String.class);
            logger.info("column字段：  " + columnList.toString());
            //List<Object>转String[]有可能报错！！！
            array = (String[]) columnList.toArray(new String[columnList.size()]);
            //从BODY体请求参数
            String bodyParam = this.writerSliceConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.BODY_PARAM);
            //query请求参数
            Map<String, Object> queryParam = this.writerSliceConfig.getMap(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.QUERY_PARAM, Object.class);

            String url = this.writerSliceConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.URL);
            String method = this.writerSliceConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.METHOD);
            String dataPath = this.writerSliceConfig.getString(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.DATA_PATH);
            //获取请求头信息
            Map<String, Object> headerInfosMap = this.writerSliceConfig.getMap(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.HEADER_INFOS, Object.class);


            //TODO...将 queryParam 的 value 替换为 record 中的 column
            replaceParam(recordReceiver, queryParam);

            //---------------------------------------------------------------------
            //logger.info("------------------------------------RecordReceiver");
            //Record record = null;
            //while ((record = recordReceiver.getFromReader()) != null) {
            //    int recordLength = record.getColumnNumber();
            //    if (0 != recordLength) {
            //        Column column;
            //        for (int i = 0; i < recordLength; i++) {
            //            column = record.getColumn(i);
            //            if (null != column.getRawData()) {
            //                logger.info("------------------------------------column:    " + column.getType() + "    " + column.getRawData().toString() + "    " + column.getByteSize());
            //            } else {
            //                logger.info("------------------------------------column:    null");
            //            }
            //        }
            //    }
            //}
            //logger.info("------------------------------------RecordReceiver");
            //---------------------------------------------------------------------


            String result;
            if (com.alibaba.datax.plugin.writer.restfulapiwriter.MethodEnum.POST.equalsIgnoreCase(method)) {
                result = com.alibaba.datax.plugin.writer.restfulapiwriter.HttpServiceUtil.insureResponsePost(url, bodyParam, headerInfosMap, queryParam);
            } else if (com.alibaba.datax.plugin.writer.restfulapiwriter.MethodEnum.GET.equalsIgnoreCase(method)) {
                result = com.alibaba.datax.plugin.writer.restfulapiwriter.HttpServiceUtil.insureResponseBlockGet(url, headerInfosMap, queryParam);
            } else {
                logger.info("请求参数不合法，必须为post/get");
                throw DataXException.asDataXException(com.alibaba.datax.plugin.writer.restfulapiwriter.RestFulApiWriterErrorCode.ILLEGAL_VALUE, String.format("您填写的参数值不合法: [%s]"));
            }

            logger.info("发送请求成功，返回数据：" + result);

        }


        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }


        public void replaceParam(RecordReceiver recordReceiver, Map<String, Object> queryParam) {

            List columnList = this.writerSliceConfig.getList(com.alibaba.datax.plugin.writer.restfulapiwriter.HttpConfigParam.COLUMN, String.class);
            Record record = recordReceiver.getFromReader();
            int recordLength = record.getColumnNumber();

            //System.out.println("------------  recordLength:" + recordLength);

            Set<Map.Entry<String, Object>> entries = queryParam.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                //System.out.println("------------  key：" + entry.getKey() + "  ,value：" + entry.getValue());

                if (null != entry.getValue()) {
                    String value = entry.getValue().toString();
                    //System.out.println("------------  value:" + value);
                    if (value.startsWith("$")) {
                        String paramName = entry.getValue().toString().substring(1, value.length());
                        //System.out.println("------------  paramName:" + paramName);

                        for (int i = 0; i < columnList.size(); i++) {
                            String column = columnList.get(i).toString();
                            //System.out.println("------------  column:" + column);

                            if (column.equals(paramName) && recordLength != 0) {
                                //queryParam.put(entry.getKey(), "\"" + record.getColumn(i).getRawData() + "\"");
                                try {
                                    //queryParam.put(entry.getKey(), "\"" + URLEncoder.encode(record.getColumn(i).getRawData().toString(), "utf-8") + "\"");
                                    //queryParam.put(entry.getKey(), URLEncoder.encode("\"" + record.getColumn(i).getRawData().toString() + "\"", "utf-8"));
                                    queryParam.put(entry.getKey(), URLEncoder.encode(record.getColumn(i).getRawData().toString(), "utf-8"));
                                } catch (Exception e) {
                                    logger.error("", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}

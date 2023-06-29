package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: DataX-master
 * @description: json解析转换
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/8/24 9:43
 */
public class JsonTransformer extends Transformer {

    private static final Logger logger = LoggerFactory.getLogger(JsonTransformer.class);

    public JsonTransformer() {
        setTransformerName("dx_json");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        logger.info("========== 开始执行json解析转换 ==========" + paras.length);

        //字段下标
        int columnIndex;
        //解析字段
        Map<Integer, String[]> parseMap;

        try {

            int parasLen = 2;
            if (paras.length < parasLen) {
                throw new RuntimeException("dx_json paras must be " + parasLen);
            }

            //获取字段下标
            columnIndex = (Integer) paras[0];
            parseMap = getParseIndexAndPath(paras[1].toString());

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }


        try {

            //获取字段
            Column column = record.getColumn(columnIndex);
            String columnStr = column.asString();

            //如果字段为空，跳过处理
            if (null == columnStr || StringUtils.isBlank(columnStr)) {
                logger.info(String.format("========== 字段为空，字段下标：%s ,字段值：%s", columnIndex, columnStr));
                return record;
            }

            //解析json
            JSONObject columnJson = JSONObject.parseObject(columnStr);
            parseJsonToColumn(columnJson, parseMap, record);

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }

        return record;
    }

    /**
     * 获取json路径和数据下标
     *
     * @param parse
     * @return
     */
    public Map<Integer, String[]> getParseIndexAndPath(String parse) {

        Map<Integer, String[]> parseMap = new HashMap<>();

        JSONObject indexAndPathJson = JSONObject.parseObject(parse);

        logger.info("==========indexAndPathJson：" + indexAndPathJson.toJSONString());

        indexAndPathJson.forEach((key, value) -> {
            String[] jsonPath = value.toString().split("\\.");
            parseMap.put(Integer.valueOf(key), jsonPath);
        });

        return parseMap;
    }

    /**
     * 读取 json 数据并放到 Record
     *
     * @param columnJson json数据
     * @param parseMap   解析路径和数据下标
     * @param record     Record
     */
    public void parseJsonToColumn(JSONObject columnJson, Map<Integer, String[]> parseMap, Record record) {

        for (Map.Entry<Integer, String[]> entry : parseMap.entrySet()) {

            String value = getJsonValue(columnJson, entry.getValue());
            record.setColumn(entry.getKey(), new StringColumn(value));

            logger.info("record.ColumnNumber:" + record.getColumnNumber());
        }
    }

    /**
     * 获取json值
     *
     * @param columnJson json数据
     * @param jsonPath   数据路径
     * @return
     */
    public String getJsonValue(JSONObject columnJson, String[] jsonPath) {

        String value = columnJson.toJSONString();
        for (int i = 0; i < jsonPath.length; i++) {
            value = getJsonValue(JSONObject.parseObject(value), jsonPath[i]);
        }

        return value;
    }

    /**
     * 获取json值
     *
     * @param json json数据
     * @param key  key
     * @return
     */
    public String getJsonValue(JSONObject json, String key) {
        return json.get(key).toString();
    }

}

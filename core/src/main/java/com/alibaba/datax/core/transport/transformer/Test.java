package com.alibaba.datax.core.transport.transformer;

import com.alibaba.fastjson2.JSONObject;

/**
 * @program: DataX-master
 * @description:
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/8/24 14:23
 */
public class Test {

    public static void main(String[] args) {

        JSONObject jsonObject = JSONObject.parseObject("{\"name\":\"李四\",\"aaa\":{\"a1\":\"111\"},\"bbb\":{\"b1\":\"222\",\"b2\":{\"b22\":\"333\"}}}");

        System.out.println(getJsonValue(jsonObject, "name".split("\\.")));
        System.out.println(getJsonValue(jsonObject, "aaa.a1".split("\\.")));
        System.out.println(getJsonValue(jsonObject, "bbb.b1".split("\\.")));
        System.out.println(getJsonValue(jsonObject, "bbb.b2.b22".split("\\.")));

    }


    public static String getJsonValue(JSONObject columnJson, String[] jsonPath) {

        if (jsonPath.length == 0) {
            return null;
        }

        String value = columnJson.toJSONString();
        for (int i = 0; i < jsonPath.length; i++) {
            value = getJsonValue(JSONObject.parseObject(value), jsonPath[i]);
        }

        return value;
    }


    public static String getJsonValue(JSONObject json, String key) {
        return json.get(key).toString();
    }
}

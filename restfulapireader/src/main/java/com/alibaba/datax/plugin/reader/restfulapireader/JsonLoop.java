package com.alibaba.datax.plugin.reader.restfulapireader;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.Map;

/**
 * @author LF
 * @date 2020/12/25 - 15:48
 */

public class JsonLoop {

    public static String str;

    public static String jsonLoop(Object object, String rows) {
        if(object instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) object;
            for (Map.Entry<String, Object> entry: jsonObject.entrySet()) {
                if (rows.equalsIgnoreCase(entry.getKey())){
                    str = entry.getValue().toString();
                }else {
                    jsonLoop(entry.getValue(), rows);
                }
            }
        }
        if(object instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) object;
            for(int i = 0; i < jsonArray.size(); i ++) {
                jsonLoop(jsonArray.get(i), rows);
            }
        }

        return str;
    }

    /*public static void main(String[] args) {
        String json = "{\n" +
                "\t\"code\":200,\n" +
                "\t\"data\":{\n" +
                "\t\t\t\"pageSize\":10,\n" +
                "\t\t\t\"currentPage\":1,\n" +
                "\t\t\t\"totalPage\":0,\n" +
                "\t\t\t\"totalCount\":1076,\n" +
                "\t\t\t\"rows\":[\n" +
                "\t\t\t\t{\"operatorAccount\":\"admin123\",\"operatorRole\":\"超级管理员\",\"operatorIp\":\"1.83.124" +
                ".112\",\"operatorDate\":\"2020-12-22 14:00:01\"}\n" +
                "\t\t\t]\n" +
                "\t\t}\n" +
                "}";
        JSONObject jsonObject = JSON.parseObject(json);
        String rows = jsonLoop(jsonObject, "rows");
        System.out.println(rows);
    }*/
}

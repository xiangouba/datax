package com.alibaba.datax.plugin.writer.hndlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import static com.alibaba.datax.plugin.writer.hndlwriter.constant.CommonConstants.*;

public class HttpServiceUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpServiceUtil.class);
    private static final Integer SUCCESS = 200;

    /**
     * 接口调用(post请求) 数据处理
     *
     * @param url       请求路径 例如：http://127.0.0.1:8080/test/test
     * @param bodyParam 请求参数 例如：{ "userName":"Lf", "password":"123456" }
     * @return 响应数据 例如：{
     * {
     * "msg": "ok",
     * "code": "200",
     * "data": [
     * {
     * "name": "lf",
     * "age": "10"
     * }
     * ]
     * }
     */
    public static Map<String,String> insureResponsePost(String url, String bodyParam) throws Exception {

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            Map<String,String> result = new HashMap<>(16);
            //推送数据
            logger.info(" #########开始推送#########");
            logger.info("推送数据：" + bodyParam);
            HttpEntity<String> request = new HttpEntity<>(bodyParam, headers);
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            logger.info("推送完成 postUrl:{}, code:{},messange:{}", url, response.getStatusCode(), response.getBody());
            result.put(MESSAGE,response.getBody());
            result.put(CODE,response.getStatusCode() + "");
            return result;

        } catch (Exception e) {
            logger.error("推送异常：", e);
            throw new Exception(e);
        }
    }

    /**
     * Http接口调用(get请求) 数据处理
     *
     * @param url 请求地址  例如：http://127.0.0.1:8080/test/test?username=zhangsan&username=123456
     * @return *          响应数据 例如：{
     * *   {
     * *     "msg": "ok",
     * *     "code": "200",
     * *     "data": [
     * *         {
     * *             "name": "lf",
     * *             "age": "10"
     * *         }
     * *     ]
     * *   }
     */
    public static String insureResponseBlockGet(String url, Map<String, Object> headerInfosMap,
                                                Map<String, Object> queryParam) {
        PrintWriter out = null;
        String result = "";
        HttpURLConnection conn = null;
        InputStream is = null;
        BufferedReader br = null;
        StringBuffer strBuffer = new StringBuffer();
        try {
            URL realUrl = new URL(queryUrl(url, queryParam));
            // 打开和URL之间的连接
            conn = (HttpURLConnection) realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(20000);
            conn.setReadTimeout(300000);
            //设置请求头参数 如token,auth... 可能存在多个
            Iterator<Map.Entry<String, Object>> iterator = headerInfosMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                conn.addRequestProperty(entry.getKey(), entry.getValue().toString());
            }
            // 传输数据为json，如果为其他格式可以进行修改
            conn.setRequestProperty("Content-Type", "application/json");
            is = conn.getInputStream();
            int code = conn.getResponseCode();
            if (SUCCESS == code) {
                br = new BufferedReader(new InputStreamReader(is));
                String line = null;
                while ((line = br.readLine()) != null) {
                    strBuffer.append(line);
                }
                result = strBuffer.toString();
            } else {
                logger.info("请求失败");
                logger.info("接口响应码：  " + code);
                logger.info("URL:  " + realUrl);
                logger.info("METHOD:  " + "GET");
                headerInfosMap.entrySet().forEach(l -> logger.info("headerInfo:  key==" + l.getKey()
                        + "  value==" + l.getValue().toString()));
                queryParam.entrySet().forEach(l -> logger.info("queryParam:  key==" + l.getKey()
                        + "  value==" + l.getValue().toString()));
                throw DataXException.asDataXException(RestFulApiWriterErrorCode.REQUEST_FAILED,
                        String.format("接口请求失败: [%s]"));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(RestFulApiWriterErrorCode.RUNTIME_EXCEPTION, String.format("出现运行时异常, 请联系我们: [%s]，\n异常信息：[%s]\n\n", HttpServiceUtil.class, e.getMessage()));
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (br != null) {
                    br.close();
                }
                if (conn != null) {
                    conn.disconnect();
                }
            } catch (IOException ex) {
                throw DataXException.asDataXException(RestFulApiWriterErrorCode.RUNTIME_EXCEPTION,
                        String.format("出现运行时异常, 请联系我们: [%s]", HttpServiceUtil.class));
            }
        }
        return result;
    }

    /**
     * @param url
     * @param queryParam
     * @return url
     * 如果queryParam不为空则拼接url
     */
    private static String queryUrl(String url, Map<String, Object> queryParam) {
        if (!queryParam.isEmpty()) {
            Iterator<Map.Entry<String, Object>> iterator = queryParam.entrySet().iterator();
            url += "?";
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                url += entry.getKey() + "=" + entry.getValue() + "&";
            }
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }
}

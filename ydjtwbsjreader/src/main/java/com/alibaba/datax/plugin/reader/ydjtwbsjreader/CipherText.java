package com.alibaba.datax.plugin.reader.ydjtwbsjreader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.gyljr.xscyl.dataHandleUtil.bo.RequestDataBO;
import com.gyljr.xscyl.dataHandleUtil.bo.ResponseDataBO;
import com.gyljr.xscyl.encryptutil.ExceptionUtil.ServiceException;
import com.gyljr.xscyl.encryptutil.SM4Util.SM4Util;
import com.gyljr.xscyl.encryptutil.SignUtil.SignUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


/**
 * @author gxx
 */
public class CipherText {

    private final static Logger logger = LogManager.getFormatterLogger(CipherText.class);

    public static ResponseDataBO generateSMResponseDataBO(Object object, String secretKey, String secretId) throws ServiceException {
        String jsonStr = JSONObject.toJSONString(object, SerializerFeature.WriteMapNullValue);
        Map<String, Object> dataMap = new HashMap();
        dataMap.put("responseResult", jsonStr);
        String encryptedDataStr = SM4Util.encryptData(jsonStr, secretKey);
        String signStr = SignUtil.createSign(dataMap, secretId);
        ResponseDataBO responseDataBO = new ResponseDataBO();
        responseDataBO.setData(encryptedDataStr);
        responseDataBO.setSignStr(signStr);
        return responseDataBO;
    }

    public static RequestDataBO generateSMRequestDataBO(Object object, String secretKey, String secretId) throws ServiceException {

        String jsonStr = JSONObject.toJSONString(object, SerializerFeature.WriteMapNullValue);
        logger.info("生成加密请求数据：" + secretKey + "    " + secretId + "    " + jsonStr);

        Map<String, Object> dataMap = new HashMap();
        dataMap.put("responseResult", jsonStr);
        String encryptedDataStr = SM4Util.encryptData(jsonStr, secretKey);
        String signStr = SignUtil.createSign(dataMap, secretId);
        RequestDataBO requestDataBO = new RequestDataBO();
        requestDataBO.setData(encryptedDataStr);
        requestDataBO.setSignStr(signStr);
        return requestDataBO;
    }

    //public static RequestData generateSMRequestData(Object object, String secretKey, String secretId) throws ServiceException {
    //
    //    String jsonStr = JSONObject.toJSONString(object, SerializerFeature.WriteMapNullValue);
    //    logger.info("生成加密请求数据：" + secretKey + "    " + secretId + "    " + jsonStr);
    //
    //    Map<String, Object> dataMap = new HashMap();
    //    dataMap.put("responseResult", jsonStr);
    //    String encryptedDataStr = SM4Util.encryptData(jsonStr, secretKey);
    //    String signStr = SignUtil.createSign(dataMap, secretId);
    //    RequestData requestData = new RequestData();
    //    requestData.setData(encryptedDataStr);
    //    requestData.setSign(signStr);
    //    return requestData;
    //}

    public static String generateSMRequestJson(Object object, String secretKey, String secretId) throws ServiceException {
        RequestDataBO requestDataBO = generateSMRequestDataBO(object, secretKey, secretId);
        //RequestData requestDataBO = generateSMRequestData(object, secretKey, secretId);
        return JSONObject.toJSONString(requestDataBO, SerializerFeature.WriteMapNullValue);
    }

    //public static void main(String[] args) {
    //
    //    String secretKey = "JFF8U9wIpOMfs2Y3";
    //    String secretId = "2cf1ae4a502d4b128ee7e649ac3473af";
    //
    //    Map<String, String> param = new HashMap<>();
    //    param.put("source", "source");
    //    param.put("interType", "interType");
    //    param.put("url", "url");
    //    param.put("companyName", "companyName");
    //
    //    try {
    //        RequestDataBO requestDataBO = generateSMRequestDataBO(param, secretKey, secretId);
    //        System.out.println(JSONObject.toJSONString(requestDataBO, SerializerFeature.WriteMapNullValue));
    //    } catch (Exception e) {
    //        logger.error("", e);
    //    }
    //}
}

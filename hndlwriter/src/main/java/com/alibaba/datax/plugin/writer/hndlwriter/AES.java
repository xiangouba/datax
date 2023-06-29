package com.alibaba.datax.plugin.writer.hndlwriter;


import com.alibaba.datax.plugin.writer.hndlwriter.constant.CommonConstants;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.alibaba.datax.plugin.writer.hndlwriter.constant.CommonConstants.PARTNER_CODE;
import static com.alibaba.datax.plugin.writer.hndlwriter.constant.CommonConstants.INSURE_TYPE;

/**
 * @Author gxx
 * @Date 2023年02月17日14时16分
 */
public class AES {
    private static final Logger logger = LoggerFactory.getLogger(AES.class);
    private static String byte2hex(byte[] b) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = (Integer.toHexString(b[n] & 0XFF));
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }
        return hs.toUpperCase();
    }

    private static byte[] hex2byte(String s) throws Exception {
        char c, c1;
        int x;
        s = s.trim();
        if (s.length() % 2 != 0) {
            throw new Exception("参数格式不正确%2不为0");
        }
        byte[] ret = new byte[s.length() / 2];

        for (int i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            c1 = s.charAt(++i);
            if (!(c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f')) {
                System.out.println("===========" + c);
                throw new Exception("参数格式不正确");
            }
            if (!(c1 >= '0' && c1 <= '9' || c1 >= 'A' && c1 <= 'F' || c1 >= 'a' && c1 <= 'f')) {
                System.out.println("------------" + c1);
                throw new Exception("参数格式不正确");
            }
            x = Integer.decode("0x" + c + c1).intValue();
            if (x > 127) {
                ret[i / 2] = (byte) (x | 0xffffff00);
            } else {
                ret[i / 2] = (byte) (x);
            }
        }
        return ret;
    }

    /**
     * 加密
     *
     * @param aeskey
     * @param data
     * @return
     * @throws Exception
     */
    public String aesEncrypt(String aeskey, String data) throws Exception {
        // Block size Both are 16
        byte[] input = data.getBytes("UTF-8");
        byte[] keyBytes = aeskey.getBytes();
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] cipherText = cipher.doFinal(input);
        return byte2hex(cipherText);

    }

    /**
     * 自定义需要加密的字段
     * @param aeskey  加密的盐值
     * @param params  加密的数据（字段之一）
     * @param columnList 需要加密的字段集合
     * @return
     * @throws Exception
     */
    public Map<String, Object> aes(String aeskey,String params, List<String> columnList) throws Exception {
        Map<String,Object> returnMap = new HashMap<>(16);
        //公共参数准备
        String partnerCode = PARTNER_CODE;
        String insuretype = INSURE_TYPE;
        //获取当前时间
        String dateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        String passTime = sdf.format(new Date());
        //接收加密后的数据
        String param = "";
        //入参封装成集合
        List<Object> paramList = new ArrayList<>();
        for (String column : columnList) {
            switch (column){
                case CommonConstants.postParam.PARTNER_CODE:
                    partnerCode = aesEncrypt(aeskey,partnerCode);
                    break;
                case CommonConstants.postParam.PASS_TIME:
                    passTime = aesEncrypt(aeskey,passTime);
                    break;
                case CommonConstants.postParam.INSURE_TYPE:
                    insuretype = aesEncrypt(aeskey,insuretype);
                    break;
                case CommonConstants.postParam.PARAMS:
                    paramList.add(params);
                    logger.info("加密前的明文数据：{}",paramList);
                    String jsonStr = JSONObject.toJSONString(paramList);
                    logger.info("加密前的json格式：{}",jsonStr);
                    //去除转义符
                        String finalJsonString = StringEscapeUtils.unescapeJava(jsonStr);
                    String paramJson = finalJsonString.substring(0,1) + finalJsonString.substring(2,finalJsonString.length()-2) +finalJsonString.substring(finalJsonString.length() - 1);
                    logger.info("去除转义符后的json：{}",paramJson);
                    param = aesEncrypt(aeskey, paramJson);
                    break;
                default:
                    break;
            }
        }
        returnMap.put("partnerCode", partnerCode);
        returnMap.put("passTime", passTime);
        returnMap.put("insuretype", insuretype);
        //接口参数
        returnMap.put("params", param);
        return returnMap;
    }

    /**
     * 解密（返回值为 string类型）
     *
     * @param aeskey
     * @param data
     * @return
     * @throws Exception
     */
    public String aesDecrypt(String aeskey, String data) throws Exception {
        byte[] cipherText = handleDecrypt(aeskey,data);
        return new String(cipherText, "UTF-8");

    }

    /**
     * 解密（返回值为 byte[]类型）
     *
     * @param aeskey
     * @param data
     * @return
     * @throws Exception
     */
    public byte[] aesDecryptByte(String aeskey, String data) throws Exception {
        return handleDecrypt(aeskey,data);
    }

    /**
     * 解密具体实现
     * @param aeskey
     * @param data
     * @return
     * @throws Exception
     */
    private byte[] handleDecrypt(String aeskey, String data) throws Exception {
        byte[] input = hex2byte(data); // Block size Both are 16
        byte[] keyBytes = aeskey.getBytes();
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] cipherText = cipher.doFinal(input);
        return cipherText;
    }
}

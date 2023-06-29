package com.alibaba.datax.plugin.writer.zhbbwriter.util;

import com.gyljr.xscyl.dataHandleUtil.bo.ResponseDataBO;
import com.gyljr.xscyl.encryptutil.ExceptionUtil.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.datax.plugin.writer.zhbbwriter.constants.Constant.CODE;
import static com.alibaba.datax.plugin.writer.zhbbwriter.constants.Constant.MESSAGE;

/**
 * @Author gxx
 * @Date 2023年06月06日14时45分
 */
public class Post {
    private static final Logger logger = LoggerFactory.getLogger(Post.class);

    /**
     * kafka 数据推送
     *
     * @param payload
     * @param url
     * @return
     * @throws Exception
     */
    public Map<String, String> process(Map<String, Object> payload, String url) throws Exception {

        String table = payload.get("table").toString();

        Map<String, Object> mutePayload = maskSensitiveField(payload);
        EncryptService encryptService = new EncryptService();
        ResponseDataBO bo = encryptService.encrypt(mutePayload);
        //地址
        url = url + table.replace("LIS.", "").toLowerCase();

        return insureResponsePost(url, bo);

    }

    /**
     * 数据库  数据推送
     *
     * @param payload
     * @param url
     * @return
     * @throws Exception
     */
    public Map<String, String> process(List<Map<String, Object>> payload, String url) throws Exception {

        EncryptService encryptService = new EncryptService();

        ResponseDataBO responseDataBO = encryptService.encryption(payload);
        //地址
        url = url + "listDevprmincom";
        return insureResponsePost(url, responseDataBO);

    }


    Map<String, Object> maskSensitiveField(Map<String, Object> payload) {
        String table = payload.get("table").toString();

        if (table != null) {
            if ("LIS.LCCONT".equalsIgnoreCase(table)) {
                //数据需要加密
                if (payload.get("APPNTNAME") != null) {
                    String str = payload.get("APPNTNAME").toString().trim();
                    //数据脱敏
                    if (str.length() > 0) {
                        payload.put("APPNTNAME", markPersonName(str));
                    }
                }

                if (payload.get("APPNTIDNO") != null) {
                    String str = payload.get("APPNTIDNO").toString().trim();

                    if (str.length() > 0) {
                        payload.put("APPNTIDNO", markShenfenzhen(str));
                    }
                }

                if (payload.get("APPNTBIRTHDAY") != null) {
//                    payload.put("APPNTBIRTHDAY", makeBirthday( new Date(1900, 1, 1)));
                    payload.put("APPNTBIRTHDAY", makeBirthday(payload.get("APPNTBIRTHDAY")));
                }

                if (payload.get("INSUREDNAME") != null) {
                    String str = payload.get("INSUREDNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("INSUREDNAME", markPersonName(str));
                    }
                }

                if (payload.get("INSUREDIDNO") != null) {
                    String str = payload.get("INSUREDIDNO").toString().trim();

                    if (str.length() > 0) {
                        payload.put("INSUREDIDNO", markShenfenzhen(str));
                    }
                }

                if (payload.get("INSUREDBIRTHDAY") != null) {
//                    payload.put("INSUREDBIRTHDAY", new Date(1900, 1, 1));
                    payload.put("INSUREDBIRTHDAY", makeBirthday(payload.get("INSUREDBIRTHDAY")));
                }
                if (payload.get("BANKACCNO") != null) {
                    String str = payload.get("BANKACCNO").toString().trim();

                    if (str.length() > 0) {
                        payload.put("BANKACCNO", makeBankNo(str));
                    }
                }

                if (payload.get("ACCNAME") != null) {
                    String str = payload.get("ACCNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("ACCNAME", markPersonName(str));
                    }
                }

                if (payload.get("NEWBANKACCNO") != null) {
                    String str = payload.get("NEWBANKACCNO").toString().trim();

                    if (str.length() > 0) {
                        payload.put("NEWBANKACCNO", makeBankNo(str));
                    }
                }

                if (payload.get("NEWACCNAME") != null) {
                    String str = payload.get("NEWACCNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("NEWACCNAME", markPersonName(str));
                    }
                }
            } else if ("LIS.LJAPAYPERSON".equalsIgnoreCase(table)) {
                // do nothing
            } else if ("LIS.LCPOL".equalsIgnoreCase(table)) {
                //数据需要加密
                if (payload.get("INSUREDNAME") != null) {
                    String str = payload.get("INSUREDNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("INSUREDNAME", markPersonName(str));
                    }
                }

                if (payload.get("INSUREDBIRTHDAY") != null) {
                    payload.put("INSUREDBIRTHDAY", makeBirthday(payload.get("INSUREDBIRTHDAY")));
                }

                if (payload.get("APPNTNAME") != null) {
                    String str = payload.get("APPNTNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("APPNTNAME", markPersonName(str));
                    }
                }


            } else if ("LIS.LCGRPCONT".equalsIgnoreCase(table)) {
                //数据需要加密

                if (payload.get("GRPNAME") != null) {
                    String str = payload.get("GRPNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("GRPNAME", markPersonName(str));
                    }
                }
//              经办人加密，20221025
                if (payload.get("HANDLERNAME") != null) {
                    String str = payload.get("HANDLERNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("HANDLERNAME", markPersonName(str));
                    }
                }
//                法人加密，20221025
                if (payload.get("CORPORATION") != null) {
                    String str = payload.get("CORPORATION").toString().trim();

                    if (str.length() > 0) {
                        payload.put("CORPORATION", markPersonName(str));
                    }
                }

                if (payload.get("PHONE") != null) {
                    String str = payload.get("PHONE").toString().trim();

                    if (str.length() > 0) {
                        payload.put("PHONE", markPhone(str));
                    }
                }

                if (payload.get("EMAIL") != null) {
                    String str = payload.get("EMAIL").toString().trim();

                    if (str.length() > 0) {
                        payload.put("EMAIL", markEmail(str));
                    }
                }

                if (payload.get("BANKACCNO") != null) {
                    String str = payload.get("BANKACCNO").toString().trim();

                    if (str.length() > 0) {
                        payload.put("BANKACCNO", makeBankNo(str));
                    }
                }
            } else if ("LIS.LCGRPPOL".equalsIgnoreCase(table)) {
                //数据需要加密

                if (payload.get("GRPNAME") != null) {
                    String str = payload.get("GRPNAME").toString().trim();

                    if (str.length() > 0) {
                        payload.put("GRPNAME", markPersonName(str));
                    }
                }
            } else if ("LIS.LDCOM".equalsIgnoreCase(table)) {
            }
        }

        return payload;
    }

    //邮箱
    private String markEmail(String str) {
        if (str.length() <= 7) {
            return "*******";
        }
        return str.substring(0, 4) + "****" + str.substring(7);
    }

    // 联系电话
    private String markPhone(String str) {
        if (str.length() <= 7) {
            return "*******";
        }
        return str.substring(0, 4) + "****" + str.substring(7);
    }

    // 客户姓名
    private String markPersonName(String str) {
    /*    if (str.length() <= 1) {
            return "*" ;
        }

        if (str.length() <= 2) {
            return str.substring(0, 1) + "*";
        }

        if (str.length() <= 3) {
            return str.substring(0, 2) + "*";
        }

        if (str.length() <= 6) {
            return str.substring(0,2) + paddingStr("*", str.length() - 2);
        }

        return str.substring(0, 2) + "****" + str.substring(str.length() - 6);*/
        return str.substring(0, 1) + "**";
    }

    // 身份证号
    private String markShenfenzhen(String str) {
        if (str.length() <= 7) {
            return "*******";
        }//:todo

//        return str.substring(0, 3) + paddingStr("*", str.length() - 7) + str.substring(str.length() - 4);
        return str.substring(0, 6) + "************";
    }

    // 统一社会信用代码
    private String markShehuixinyong(String str) {
        if (str.length() < 4) {
            return "****";
        }

        if (str.length() <= 8) {
            return "********";
        }

        return str.substring(0, 4) + paddingStr("*", str.length() - 8) + str.substring(str.length() - 4);
    }

    // 银行账号
    private String makeBankNo(String str) {
        if (str.length() < 4) {
            return "****";
        }

        if (str.length() <= 7) {
            return "********";
        }
        return "********";
    }

    private String makeBirthday(Object date) {
        if (null == date) {
            return null;
        } else if ("date".equalsIgnoreCase(date.getClass().getSimpleName())) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
            return simpleDateFormat.format(date);
        } else if ("String".equalsIgnoreCase(date.getClass().getSimpleName())) {
            if ((date).toString().length() >= 4) {
                return date.toString().substring(0, 4);
            }
        }
        return null;

    }

    private String paddingStr(String c, int count) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < count; i++) {
            sb.append(c);
        }

        return sb.toString();
    }

    private Map<String, String> insureResponsePost(String url, ResponseDataBO responseDataBO) throws Exception {

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            Map<String, String> result = new HashMap<>(16);
            //推送数据
            logger.info(" #########开始推送#########");
            logger.info("推送数据：" + responseDataBO);
            HttpEntity<ResponseDataBO> request = new HttpEntity<>(responseDataBO, headers);
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<ResponseDataBO> response = restTemplate.postForEntity(url, request, ResponseDataBO.class);
            logger.info("推送完成 postUrl:{}, code:{},messange:{}", url, response.getStatusCode(), response.getBody());
            result.put(MESSAGE, response.getBody().getMessage());
            result.put(CODE, response.getStatusCode() + "");
            return result;

        } catch (Exception e) {
            logger.error("推送异常：", e);
            throw new Exception(e);
        }
    }
}



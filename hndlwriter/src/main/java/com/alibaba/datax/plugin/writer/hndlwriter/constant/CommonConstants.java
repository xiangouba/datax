package com.alibaba.datax.plugin.writer.hndlwriter.constant;


public final class CommonConstants {

    public static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String AES_KEY = "f33b3faf1e90a3f2";
    public static final String CODE = "code";
    public static final String MESSAGE = "message";
    public static final String PASS_TIME = "passTime";
    public static final String PUSH_CODE = "pushCode";
    public static final String PUSH_MESSAGE = "pushMessage";
    public static final String PUSH_URL = "pushUrl";
    public static final String RESPONSE_CODE = "responseCode";
    public static final String RESPONSE_MESSAGE = "responseMessage";
    public static final String COLUMN_NAME = "columnName";
    public static final String COLUMN_TYPE = "columnType";
    public static final String PARTNER_CODE = "YDRS001";
    /*人身险类*/
    public static final String INSURE_TYPE = "2";

    /**
     * 推送远端接口参数常量
     */
    public static final class postParam{
        public static final String PARTNER_CODE = "partnerCode";
        public static final String PASS_TIME = "passTime";
        public static final String INSURE_TYPE = "insuretype";
        public static final String PARAMS = "params";
    }

    /**
     * 同步材料接口
     */
    public static final String DATA_URL = "http://211.160.72.202:18088/interSafeinfoPost/payFileUpload";
    public static final String PAY_FILE = "payFileUpload";


}

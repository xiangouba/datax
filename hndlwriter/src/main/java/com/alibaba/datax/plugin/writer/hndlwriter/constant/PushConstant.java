package com.alibaba.datax.plugin.writer.hndlwriter.constant;

public class PushConstant {

    /**
     *推送状态
     */
    public static final String PUSH_CODE_SUCCESS = "0";
    public static final String PUSH_CODE_FAIL = "1";
    public static final String PUSH_CODE_ERROR = "2";
    /**
     * 推送信息
     */
    public static final String PUSH_MESSAGE_SUCCESS = "推送成功";
    public static final String PUSH_MESSAGE_FAIL = "推送失败";
    public static final String PUSH_MESSAGE_ERROR = "推送异常";

    /**
     * 推送时空值的替换值
     */
    public static final String PUSH_NUll_VALUE_STRING = "无";
    public static final String PUSH_NULL_VALUE_FLOAT = "0.00";
    public static final String PUSH_NULL_VALUE_INT = "0";
    public static final String PUSH_NULL_VALUE_TIME = "1997-01-01 00:00:00";
    public static final String PUSH_NULL_VALUE_BANKCARD = "6211112223333333334";

}

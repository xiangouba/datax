package com.alibaba.datax.plugin.writer.hndlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author rh
 */

public enum RestFulApiWriterErrorCode implements ErrorCode {
    /**
     * 数据转换失败或不支持此类型
     */
    DATA_CONVERSION_FAILED("restfulapiwriter-00", "数据转换失败或不支持此类型"),
    /**
     * 获取数据失败
     */
    DATA_QUERY_FAILED("restfulapiwriter-05", "获取数据失败"),
    /**
     * 接口请求失败
     */
    REQUEST_FAILED("restfulapiwriter-06", "接口请求失败"),
    /**
     * 您填写的参数值不合法
     */
    ILLEGAL_VALUE("restfulapiwriter-01", "您填写的参数值不合法"),
    /**
     * 您的参数配置错误
     */
    CONFIG_INVALID_EXCEPTION("restfulapiwriter-02", "您的参数配置错误"),
    /**
     * 出现运行时异常, 请联系我们
     */
    RUNTIME_EXCEPTION("restfulapiwriter-03", "出现运行时异常, 请联系我们"),
    /**
     * 空值出现异常, 请联系我们
     */
    IS_NULL("restfulapiwriter-04", "空值出现异常, 请联系我们");

    private final String code;
    private final String description;

    private RestFulApiWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}

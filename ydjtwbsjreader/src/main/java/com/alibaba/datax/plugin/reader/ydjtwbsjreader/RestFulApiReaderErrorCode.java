package com.alibaba.datax.plugin.reader.ydjtwbsjreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author rh
 */

public enum RestFulApiReaderErrorCode implements ErrorCode {
    /**
     * 数据转换失败或不支持此类型
     */
    DATA_CONVERSION_FAILED("RestFulApiReader-00", "数据转换失败或不支持此类型"),


    /**
     * 接口返回code值错误
     */
    DATA_CODE_FAILED("YdjtWbsjReader-00", "接口返回code值错误"),
    /**
     * 获取数据失败
     */
    DATA_QUERY_FAILED("RestFulApiReader-05", "获取数据失败"),
    /**
     * 接口请求失败
     */
    REQUEST_FAILED("RestFulApiReader-06", "接口请求失败"),
    /**
     * 您填写的参数值不合法
     */
    ILLEGAL_VALUE("RestFulApiReader-01", "您填写的参数值不合法"),
    /**
     * 您的参数配置错误
     */
    CONFIG_INVALID_EXCEPTION("RestFulApiReader-02", "您的参数配置错误"),
    /**
     * 出现运行时异常, 请联系我们
     */
    RUNTIME_EXCEPTION("RestFulApiReader-03", "出现运行时异常, 请联系我们"),
    /**
     * 空值出现异常, 请联系我们
     */
    IS_NULL("RestFulApiReader-04", "空值出现异常, 请联系我们");

    private final String code;
    private final String description;

    private RestFulApiReaderErrorCode(String code, String description) {
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

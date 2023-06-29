package com.alibaba.datax.plugin.reader.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaWriterErrorCode implements ErrorCode {
    //TOPIC_ERROR
    REQUIRED_VALUE("KafkaWriter-00", "您缺失了必须填写的参数值."),
    //KAFKA_READER_ERROR
    KAFKA_WRITER_ERROR("KafkaReader-01","Kafka writer错误"),;

    private final String code;
    private final String description;

    private KafkaWriterErrorCode(String code, String description) {
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

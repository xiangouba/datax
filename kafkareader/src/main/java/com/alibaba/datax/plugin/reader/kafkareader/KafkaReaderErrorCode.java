package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author yuejinfu
 */

public enum KafkaReaderErrorCode implements ErrorCode {
    //TOPIC_ERROR
    TOPIC_ERROR("KafkaReader-01","Topic错误"),
    //PARTITION_ERROR
    PARTITION_ERROR("KafkaReader-02","分区错误"),
    //ADDRESS_ERROR
    ADDRESS_ERROR("KafkaReader-03","地址错误"),
    //KAFKA_READER_ERROR
    KAFKA_READER_ERROR("KafkaReader-04","Kafka reader错误"),;

    private final String code;
    private final String description;

    private KafkaReaderErrorCode(String code, String description) {
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

package com.alibaba.datax.plugin.writer.sjsqwriter.entity;

/**
 * 记录表
 */

public class RsRecord {
    /**
     * 表名
     */
    private String table_name;
    /**
     * md5
     */
    private String md5code;
    /**
     * 操作：insert，delete
     */
    private String opt;

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getMd5code() {
        return md5code;
    }

    public void setMd5code(String md5code) {
        this.md5code = md5code;
    }

    public String getOpt() {
        return opt;
    }

    public void setOpt(String opt) {
        this.opt = opt;
    }

    public RsRecord(String table_name, String md5code, String opt) {
        this.table_name = table_name;
        this.md5code = md5code;
        this.opt = opt;
    }

    public RsRecord() {
    }
}

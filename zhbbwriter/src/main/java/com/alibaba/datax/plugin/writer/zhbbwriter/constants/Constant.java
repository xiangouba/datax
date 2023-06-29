package com.alibaba.datax.plugin.writer.zhbbwriter.constants;

/**
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public final class Constant {
    public static final int DEFAULT_BATCH_SIZE = 2048;
    public static final int SGCC_BATCH_SIZE = 50;

    public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;

    public static String TABLE_NAME_PLACEHOLDER = "@table";

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

    public static String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";
    public static final String CODE = "code";
    public static final String MESSAGE = "message";

    public static final String IS_KAFKA = "0";
    public static final String ISNOT_KAFKA = "1";

    public static final String COLUMN_NAME = "columnName";
    public static final String COLUMN_TYPE = "columnType";
}

package com.alibaba.datax.plugin.reader.kafkareader;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String toStringByFormat(Date date, String format) {
        String dateStr = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            dateStr = sdf.format(date);
        } catch (Exception e) {
        }
        return dateStr;
    }


    public static String toString(Date date) {
        String dateStr = null;
        try {
            dateStr = sdf.format(date);
        } catch (Exception e) {

        }
        return dateStr;
    }
}
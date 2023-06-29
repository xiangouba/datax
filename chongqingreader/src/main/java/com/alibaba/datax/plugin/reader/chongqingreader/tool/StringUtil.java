package com.alibaba.datax.plugin.reader.chongqingreader.tool;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @auther: rihang
 * @create: 2022-06-30
 * @Description: 针对string类型进行操作的一些util
 */
public class StringUtil {


    /**
     * @param c      用于补位的字符
     * @param length 补位的长度
     * @param target 需要补位的字符串
     * @return
     * @author rihang
     * @description 在给定的字符右边补充字符到指定的位数
     */
    public static String flushright(String c, int length, String target) {
        StringBuilder stringBuilder = new StringBuilder();
        if (target != null) {
            stringBuilder.append(target);
            for (int index = 0; index < length - target.length(); index++) {
                stringBuilder.append(c);
            }
        } else {
            for (int index = 0; index < length; index++) {
                stringBuilder.append(c);
            }
        }
        return stringBuilder.toString();
    }

    /**
     * @param c      用于补位的字符
     * @param length 补位的长度
     * @param target 需要补位的字符串
     * @return
     * @author rihang
     * @description 在给定的字符左边补充字符到指定的位数
     */
    public static String flushleft(String c, int length, String target) {
        StringBuilder stringBuilder = new StringBuilder();
        if (target != null) {
            for (int index = 0; index < length - target.length(); index++) {
                stringBuilder.append(c);
            }
            stringBuilder.append(target);
        } else {
            for (int index = 0; index < length; index++) {
                stringBuilder.append(c);
            }
        }
        return stringBuilder.toString();
    }

    public static String getUUid() {
        String uuid = UUID.randomUUID().toString();
        return uuid.replace("-", "");
    }

    public static String getSqlvalue(String type, List<String> idnos) {
        StringBuilder stringBuilder = new StringBuilder();
        String polno = "polno ";
        if ("1".equals(type)|| "lipei".equals(type)){
            polno  = "b.appntidno";
        } else if ("2".equals(type)|| "chengbao".equals(type)){
            polno  = "b.appntidno ";
        } else   if ("3".equals(type)|| "baoquan".equals(type)){
            polno  = " appntidno  ";
        }else if ("11".equals(type)|| "lipeimingxi".equals(type)){
            polno  = " e.PayeeIDNo ";
        }else if ("12".equals(type)|| "lipeimingxi".equals(type)){
            polno  = " e.idno ";
        }else if ("13".equals(type)|| "lipeimingxi".equals(type)){
            polno  = "b.appntidno ";
        }else {
            return null;
        }
        stringBuilder.append("  ( " +                "  ");
        int grnu =100;
        ArrayList<String> strings1 = new ArrayList<>();
        for (int index = 0; index < idnos.size(); index++) {
            if (index == 0 && idnos.size()==1  ){
                stringBuilder.append(" " + polno + "="+ "'" +idnos.get(index) +"'" );
            }else if (index == 0  ){
                stringBuilder.append(" " + polno + " in " );
            } else if (((index + 1) % grnu) == 1 ){
                stringBuilder.append(" or  " + polno + "in " );
//            stringBuilder.append("" +   idnos.get(index) + "," );
            }
            strings1.add("'" + idnos.get(index) + "'");
            if (((index + 1) % grnu) == 0 ){
                stringBuilder.append("( ");
                stringBuilder.append(String.join(",",strings1.stream().map(String::valueOf).collect(Collectors.toList())) );
                stringBuilder.append(" ) ");
                strings1.clear();
            }
        }
        if (strings1.size()!= 0 &&  idnos.size()!=1){
            stringBuilder.append("( ");
            stringBuilder.append(String.join(",",strings1.stream().map(String::valueOf).collect(Collectors.toList())) );
            stringBuilder.append(" ) ");
        }
//        if (((idnos.size() + 1) % 2) == 0) {
//            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
//        }
        stringBuilder.append(" ) ");
            return stringBuilder.toString();
        }

}

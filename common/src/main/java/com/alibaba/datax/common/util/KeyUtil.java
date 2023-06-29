package com.alibaba.datax.common.util;

import java.util.Random;

/**
 * @program: DataX-master
 * @description:
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/8/18 14:59
 */
public class KeyUtil {
    /**
     * 生成唯一的主键，格式就是时间—+随机数
     *
     * @return
     */
    public static synchronized String genUniqueKey() {

        Random random = new Random();
        //随机生成一个六位数字
        Integer number = random.nextInt(900000) + 100000;

        //lic int nextInt(int n)
        //该方法的作用是生成一个随机的int值，该值介于[0,n)的区间，也就是0到n之间的随机int值，包含0而不包含n。
        return System.currentTimeMillis() + String.valueOf(number);
    }
}


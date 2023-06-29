package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataXCaseEnvUtil {

    private static final Logger logger = LoggerFactory.getLogger(DataXCaseEnvUtil.class);

    // datax回归测试效率提升
    private static String DATAX_AUTOTEST_RETRY_TIME = System.getenv("DATAX_AUTOTEST_RETRY_TIME");
    private static String DATAX_AUTOTEST_RETRY_INTERVAL = System.getenv("DATAX_AUTOTEST_RETRY_INTERVAL");
    private static String DATAX_AUTOTEST_RETRY_EXPONENTIAL = System.getenv("DATAX_AUTOTEST_RETRY_EXPONENTIAL");

    public static int getRetryTimes(int retryTimes) {
        int actualRetryTimes = DATAX_AUTOTEST_RETRY_TIME != null ? Integer.valueOf(DATAX_AUTOTEST_RETRY_TIME) : retryTimes;
       // logger.info("The actualRetryTimes is {}", actualRetryTimes);
        return actualRetryTimes;
    }

    public static long getRetryInterval(long retryInterval) {
        long actualRetryInterval = DATAX_AUTOTEST_RETRY_INTERVAL != null ? Long.valueOf(DATAX_AUTOTEST_RETRY_INTERVAL) : retryInterval;
       // logger.info("The actualRetryInterval is {}", actualRetryInterval);
        return actualRetryInterval;
    }

    public static boolean getRetryExponential(boolean retryExponential) {
        boolean actualRetryExponential = DATAX_AUTOTEST_RETRY_EXPONENTIAL != null ? Boolean.valueOf(DATAX_AUTOTEST_RETRY_EXPONENTIAL) : retryExponential;
       // logger.info("The actualRetryExponential is {}", actualRetryExponential);
        return actualRetryExponential;
    }
}

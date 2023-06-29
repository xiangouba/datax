package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @program: DataX-master
 * @description:
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/8/18 14:50
 */
public class ShellUtil {

    private static final Logger logger = LoggerFactory.getLogger(ShellUtil.class);
    private static final int SUCCESS = 0;

    public static boolean exec(String[] command) {
        try {
            Process process = Runtime.getRuntime().exec(command);
            read(process.getInputStream());
            StringBuilder errMsg = read(process.getErrorStream());
            // 等待程序执行结束并输出状态
            int exitCode = process.waitFor();
            if (exitCode == SUCCESS) {
                logger.info("脚本执行成功");
                return true;
            } else {
                logger.info("脚本执行失败[ERROR]:" + errMsg.toString());
                return false;
            }
        } catch (Exception e) {
            logger.error("脚本执行异常：",e);
        }
        return false;
    }

    private static StringBuilder read(InputStream inputStream) {
        StringBuilder resultMsg = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                resultMsg.append(line);
                resultMsg.append("\r\n");
            }
            return resultMsg;
        } catch (IOException e) {
            logger.error("",e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("",e);
            }
        }
        return null;
    }

}

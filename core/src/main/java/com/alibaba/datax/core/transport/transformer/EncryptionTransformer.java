package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.encryption.EncryptionConstant;
import com.alibaba.datax.core.util.encryption.Sm4Util;
import com.alibaba.datax.transformer.Transformer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * @program: DataX-master
 * @description: 加密转换
 * @author: LPH
 * @version: 1.0
 * @Create: 2022/7/26 15:36
 */
public class EncryptionTransformer extends Transformer {

    private static final Logger logger = LoggerFactory.getLogger(EncryptionTransformer.class);

    public EncryptionTransformer() {
        setTransformerName("dx_encryption");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        logger.info("========== 开始执行加密转换 ==========" + paras.length);

        //字段下标
        int columnIndex;
        //加密算法
        String algorithm;
        //类型：encrypt,decrypt
        String type;

        try {

            int parasLen = 3;
            if (paras.length < parasLen) {
                throw new RuntimeException("dx_encryption paras must be " + parasLen);
            }

            //获取字段下标、加密算法、类型
            columnIndex = (Integer) paras[0];
            algorithm = (String) paras[1];
            type = (String) paras[2];

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        try {
            //获取字段
            Column column = record.getColumn(columnIndex);
            String columnStr = column.asString();

            //如果字段为空，跳过处理
            if (null == columnStr || StringUtils.isBlank(columnStr)) {

                logger.info(String.format("========== 字段为空，字段下标：%s ,字段值：%s", columnIndex, columnStr));
                return record;
            }

            //进行加密
            String newValue = encryption(columnStr, algorithm, type, paras);
            logger.info("========== 加密：" + columnStr + " ---> " + newValue);

            //将加密后的数据重新写入
            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }

        return record;
    }

    /**
     * @param oriValue  密文
     * @param algorithm 算法
     * @param type      加密类型：encrypt,decrypt
     * @param paras     参数
     */
    public String encryption(String oriValue, String algorithm, String type, Object... paras) {

        logger.info(String.format("========== 密文：%s ,算法：%s ,类型:%s ", oriValue, algorithm, type));

        if (algorithm.equals(EncryptionConstant.ALGORITHM_SM4)) {
            return encryptionSM4(oriValue, algorithm, type, paras);
        } else {
            throw new RuntimeException("未指定加密算法");
        }
    }

    public String encryptionSM4(String oriValue, String algorithm, String type, Object... paras) {

        String newStr = null;

        if (algorithm.equals(EncryptionConstant.ALGORITHM_SM4)) {
            //加密
            if (type.equals(EncryptionConstant.TYPE_ENCRYPT)) {
                int parasLength_1 = 3;
                int parasLength_2 = 3 + 1;
                if (paras.length == parasLength_1) {
                    //使用默认秘钥加密
                    newStr = Sm4Util.encryptEcb(oriValue);
                } else if (paras.length == parasLength_2) {
                    //指定秘钥加密
                    String hexKey = (String) paras[parasLength_2 - 1];
                    newStr = Sm4Util.encryptEcb(hexKey, oriValue);
                } else {
                    throw new RuntimeException("dx_encryption " + algorithm + "  " + type + " paras must be " + parasLength_1 + " or " + parasLength_2);
                }
            }
            //解密
            else if (type.equals(EncryptionConstant.TYPE_DECRYPT)) {
                int parasLength_1 = 3;
                int parasLength_2 = 3 + 1;
                if (paras.length == parasLength_1) {
                    //使用默认秘钥解密
                    newStr = Sm4Util.decryptEcb(oriValue);
                } else if (paras.length == parasLength_2) {
                    //指定秘钥解密
                    String hexKey = (String) paras[parasLength_2 - 1];
                    newStr = Sm4Util.decryptEcb(hexKey, oriValue);
                } else {
                    throw new RuntimeException("dx_encryption " + algorithm + "  " + type + " paras must be " + parasLength_1 + " or " + parasLength_2);
                }
            }
        }

        return newStr;
    }

}

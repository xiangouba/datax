package com.alibaba.datax.core.util.encryption;

import java.security.Key;
import java.security.Security;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Sm4Util {
    private static String priKey = "369F5560201C4E11A2B825F9583C47F5";
    private static String pubKey = "369F5560201C4E11A2B825F9583C47F5";


    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final String ENCODING = "UTF-8";
    public static final String ALGORITHM_NAME = "SM4";
    public static final String ALGORITHM_NAME_ECB_PADDING = "SM4/ECB/PKCS5Padding";
    public static final int DEFAULT_KEY_SIZE = 128;

    // 生成ECB暗号
    private static Cipher generateEcbCipher(String algorithmName, int mode, byte[] key) throws Exception {
        Cipher cipher = Cipher.getInstance(algorithmName, BouncyCastleProvider.PROVIDER_NAME);
        Key sm4Key = new SecretKeySpec(key, ALGORITHM_NAME);

        cipher.init(mode, sm4Key);
        return cipher;
    }

    // sm4加密
    public static String encryptEcb(String hexKey, String paramStr) {
        try {
            String cipherText = "";
            byte[] keyData = ByteUtils.fromHexString(hexKey);
            byte[] srcData = paramStr.getBytes(ENCODING);
            // 加密后的数组
            byte[] cipherArray = encrypt_Ecb_Padding(keyData, srcData);
            cipherText = ByteUtils.toHexString(cipherArray);
            return cipherText;
        } catch (Exception e) {
            return paramStr;
        }
    }

    // sm4加密
    public static String encryptEcb(String paramStr) {
        try {
            String hexKey = priKey;
            if (hexKey == null) {
                hexKey = "369F5560201C4E11A2B825F9583C47F6";
            }
            String cipherText = "";
            byte[] keyData = ByteUtils.fromHexString(hexKey);
            byte[] srcData = paramStr.trim().getBytes(ENCODING);
            // 加密后的数组
            byte[] cipherArray = encrypt_Ecb_Padding(keyData, srcData);
            cipherText = ByteUtils.toHexString(cipherArray);
            return cipherText;
        } catch (Exception e) {
            return paramStr;
        }
    }

    // 加密模式之Ecb
    public static byte[] encrypt_Ecb_Padding(byte[] key, byte[] data) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    // sm4解密
    public static String decryptEcb(String hexKey, String cipherText) {
        // 用于接收解密后的字符串
        String decryptStr = "";
        byte[] keyData = ByteUtils.fromHexString(hexKey);
        byte[] cipherData = ByteUtils.fromHexString(cipherText);
        // 解密
        byte[] srcData = new byte[0];
        try {
            srcData = decrypt_Ecb_Padding(keyData, cipherData);
            decryptStr = new String(srcData, ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return decryptStr;
    }

    // sm4解密
    public static String decryptEcb(String cipherText) {
        String hexKey = priKey;
        // 用于接收解密后的字符串
        String decryptStr = "";
        if (hexKey == null) {
            hexKey = "369F5560201C4E11A2B825F9583C47F6";
        }
        byte[] keyData = ByteUtils.fromHexString(hexKey);
        byte[] cipherData = ByteUtils.fromHexString(cipherText);
        // 解密
        byte[] srcData = new byte[0];
        try {
            srcData = decrypt_Ecb_Padding(keyData, cipherData);
            decryptStr = new String(srcData, ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return decryptStr;
    }

    // 解密
    public static byte[] decrypt_Ecb_Padding(byte[] key, byte[] cipherText) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(cipherText);
    }

    /**
     * @param hexKey 密钥 , cipherText  加密后数据，     paramStr  加密前数据，
     * @Return: 是否是同一数据，true，是，fale,否
     * @Description: 校验加密前后的字符串是否为同一数据
     */
    public static boolean verifyEcb(String hexKey, String cipherText, String paramStr) throws Exception {
        boolean flag = false;
        byte[] keyData = ByteUtils.fromHexString(hexKey);
        byte[] cipherData = ByteUtils.fromHexString(cipherText);
        // 解密
        byte[] decryptData = decrypt_Ecb_Padding(keyData, cipherData);
        byte[] srcData = paramStr.getBytes(ENCODING);
        flag = Arrays.equals(decryptData, srcData);
        return flag;
    }

    //   测试
    public static void main(String[] args) {
        t1();
        System.out.println("=========================================================================");
        t2();
    }

    public static void t2() {
        try {
            String json = "223.000234";
            System.out.println("加密前源数据:" + json);

            System.out.println("默认秘钥：" + priKey);

            String cipher = Sm4Util.encryptEcb(json);
            System.out.println("加密串:" + cipher);
            System.out.println("加密串:" + cipher.length());
            String jsonnew = Sm4Util.decryptEcb(cipher);
            System.out.println("解密后数据：" + jsonnew);
            System.out.println(verifyEcb(priKey, cipher, json));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void t1() {
        try {
            String json = "223.000234";
            System.out.println("加密前源数据:" + json);
            String key = "369F5560201C4E11A2B825F9583C47F6";
//            String key = "369F5560201C4E11A2B825F9583C47F6";
            String cipher = Sm4Util.encryptEcb(key, json);
            System.out.println("加密串:" + cipher);
            System.out.println("加密串:" + cipher.length());
            String jsonnew = Sm4Util.decryptEcb(key, cipher);
            System.out.println("解密后数据：" + jsonnew);
            System.out.println(verifyEcb(key, cipher, json));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

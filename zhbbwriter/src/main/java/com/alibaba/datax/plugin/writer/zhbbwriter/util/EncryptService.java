package com.alibaba.datax.plugin.writer.zhbbwriter.util;

import com.gyljr.xscyl.dataHandleUtil.DataHandleUtil;
import com.gyljr.xscyl.dataHandleUtil.bo.ResponseDataBO;
import com.gyljr.xscyl.encryptutil.ExceptionUtil.ServiceException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class EncryptService {
    private final static Logger logger = LogManager.getFormatterLogger(EncryptService.class);

    private String secretKey = "JFF8U9wIpOMfs2Y3";

    private String secretId = "2cf1ae4a502d4b128ee7e649ac3473af";

    public ResponseDataBO encrypt(Map<String, Object> payload) throws ServiceException {
        ResponseDataBO bo = DataHandleUtil.generateSMResponseDataBO(payload, secretKey, secretId);

        return bo;
    }

    /**
     * 加密处理
     *
     * @param mapList
     * @return
     */
    public ResponseDataBO encryption(List<Map<String, Object>> mapList) {
        try {
            ResponseDataBO bo = DataHandleUtil.generateSMResponseDataBO(mapList, secretKey, secretId);
            return bo;
        } catch (Exception e) {
            logger.error("", e);
        }

        return null;
    }
}

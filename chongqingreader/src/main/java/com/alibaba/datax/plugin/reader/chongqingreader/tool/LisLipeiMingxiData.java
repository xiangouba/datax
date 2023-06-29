package com.alibaba.datax.plugin.reader.chongqingreader.tool;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @auther: rihang
 * @create: 2022-07-06
 * @Description: 理赔明细数据（一年内）
 * 说明：
 * ①关联理赔数据，查询一年内理赔明细数据。
 * 表名 year_claim
 * 参数名 必填 类型范围 说明
 */
@Data
@Slf4j
public class LisLipeiMingxiData {


    private String id;    //是 String 理赔表主键（关联理赔表）
    private String claim_id;    //是 String 理赔表主键（关联理赔表）
    private String report_no;   //是 String 报案号
    private String premium;     //是 Number 理赔金额（保留两位小数）(加密)
    private String claim_time;  //是 String 理赔时间 YYYY-MM-DD
    private String claim_reason;//是 String 理赔结论


    public void convert_Enaty(Map<String, Object> datas) {
        this.id = StringUtil.getUUid();
        this.claim_id = (String) datas.get("claim_id".toUpperCase());
        this.report_no = (String) datas.get("report_no".toUpperCase());
        String premium = (String) datas.get("premium".toUpperCase());//加密
//        this.premium = bigDecimal; //  bigDecimal.doubleValue();
        this.premium = Sm4Util.encryptEcb(premium); //  bigDecimal.doubleValue();

        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date1 = (Date) datas.get("claim_time".toUpperCase());
            if (null != date1) {
                this.claim_time = simpleDateFormat.format(date1);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        this.claim_reason = (String) datas.get("claim_reason".toUpperCase());
    }


}

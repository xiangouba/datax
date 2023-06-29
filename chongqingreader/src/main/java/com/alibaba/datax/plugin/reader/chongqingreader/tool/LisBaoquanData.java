package com.alibaba.datax.plugin.reader.chongqingreader.tool;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @auther: rihang
 * @create: 2022-07-06
 * @Description: 保全数据
 *  保单贷款数据
 * 说明：
 * ①以投保人身份证号码为条件，查询所有未还款和三年以内已还款的贷款信
 * 息
 * ②没有该模块的保险公司视为查询无结果，即 result_state 为 0
 * ③同一保单下险种很多时，只显示主险种(险种名称同)
 * 表名 loan_result
 * 参数名 必填 类型范围 说明
 */
@Data
@Slf4j
public class LisBaoquanData {

    private String id;
    private String batch_no;                    //  是 String 批次号(请求字段原数据返回即可）
    private String commission_id;                   //  是 String 记录 ID(请求字段原数据返回即可)
    private String people_id;                   //  是 String 人员 ID(请求字段原数据返回即可)
    private String corporate_name = "英大泰和人寿";                  //  是 String 保险公司名称
    private String result_state = "1";                    //  是 String “0”代表查询无结果，“1”查询有结果（无结果时后续字段不必填）
    private String policy_no;                   //  是 String 保单号
    private String policy_holder;                   //  是 String 投保人姓名 //加密
    private String policy_holder_id_card;                   //  是 String 投保人身份证号码 //加密
    private String insurance_type;                  //  是 String 险种（中文险种分类名，以各保险公司险种分类为准）
    private String insurance_name;                  //  是 String 保险名称
    private String accumulate_premium;                  //  是 累交保费（元）（按保单号汇总保费， 保留两位小数）
    private String loan_amount;                 //  是 String 保单贷款金额(保留两位小数) 加密
    private String loan_time;                   //  是 String 保单贷款时间 YYYY-MM-DD
    private String return_amount;                   // 否 String 保单还款金额(保留两位小数)（已还贷款此项必填） 加密
    private String return_time;                 //  否 String 还款时间 YYYY-MM-DD（已还贷款此项必填）

    public void convert_Enaty( String batch_no, String commission_id, String people_id) {
        this.id = StringUtil.getUUid();
        this.setBatch_no(batch_no);
        this.setCommission_id(commission_id);
        this.setPeople_id(people_id);
        this. result_state = "0";
    }

    public void convert_Enaty(Map<String, Object> datas, String batch_no, String commission_id, String people_id) {
        this.id = StringUtil.getUUid();
        this.setBatch_no(batch_no);
        this.setCommission_id(commission_id);
        this.setPeople_id(people_id);

//        this.setCorporate_name((String) datas.get("corporate_name".toUpperCase()));
//        this.setResult_state((String) datas.get("result_state".toUpperCase()));
        this.setPolicy_no((String) datas.get("policy_no".toUpperCase()));

        this.setInsurance_type((String) datas.get("insurance_type".toUpperCase()));
        this.setInsurance_name((String) datas.get("insurance_name".toUpperCase()));


        try {
//            this.setLoan_time((String) datas.get("loan_time".toUpperCase()));
//            this.setReturn_time((String) datas.get("".toUpperCase()));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date1 = (Date) datas.get("loan_time".toUpperCase());

            if (null != date1) {
                this.loan_time = simpleDateFormat.format(date1);
            }

            Date date2 = (Date) datas.get("return_time".toUpperCase());
//            (String) data.get("last_claim_time".toUpperCase());
            if (date2 != null) {
                this.return_time = simpleDateFormat.format(date2);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


//            this.setPolicy_holder((String) datas.get("policy_holder".toUpperCase()));//加密
        this.setPolicy_holder(Sm4Util.encryptEcb((String) datas.get("policy_holder".toUpperCase())));//加密
//            this.setPolicy_holder_id_card((String) datas.get("policy_holder_id_card".toUpperCase())); //加密
        this.setPolicy_holder_id_card(Sm4Util.encryptEcb((String) datas.get("policy_holder_id_card".toUpperCase()))); //加密


        this.setAccumulate_premium(Sm4Util.encryptEcb((String) datas.get("accumulate_premium".toUpperCase())));  //加密
        String bigDecimal = (String) datas.get("loan_amount".toUpperCase());
        if (bigDecimal != null) {
            String bigDecimala = Sm4Util.encryptEcb(bigDecimal.toString());
            this.setLoan_amount(bigDecimala); //加密
//                this.setLoan_amount(bigDecimal.toString()); //加密
        } else {
            String bigDecimala = Sm4Util.encryptEcb("0.00");
            this.setLoan_amount(bigDecimala); //加密
        }
        String return_amount = (String) datas.get("return_amount".toUpperCase());
        if (return_amount == null) {
            return_amount = "0.00";
        }
        this.setReturn_amount(Sm4Util.encryptEcb(return_amount)); //加密


    }

    public void convert_Enaty(Map<String, Object> datas, String batch_no, String commission_id, String people_id, String result_state) {
        this.id = StringUtil.getUUid();
        this.setBatch_no(batch_no);
        this.setCommission_id(commission_id);
        this.setPeople_id(people_id);

//        this.setCorporate_name((String) datas.get("corporate_name".toUpperCase()));
//        this.setResult_state((String) datas.get("result_state".toUpperCase()));
        this.setResult_state(result_state);
        if ("1".equals(result_state)) {
            this.setPolicy_no((String) datas.get("policy_no".toUpperCase()));

            this.setInsurance_type((String) datas.get("insurance_type".toUpperCase()));
            this.setInsurance_name((String) datas.get("insurance_name".toUpperCase()));

            try {
//            this.setLoan_time((String) datas.get("loan_time".toUpperCase()));
//            this.setReturn_time((String) datas.get("".toUpperCase()));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date date1 = (Date) datas.get("loan_time".toUpperCase());

                if (null != date1) {
                    this.loan_time = simpleDateFormat.format(date1);
                }

                Date date2 = (Date) datas.get("return_time".toUpperCase());
//            (String) data.get("last_claim_time".toUpperCase());
                if (date2 != null) {

                    this.return_time = simpleDateFormat.format(date2);
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            String cipher = Sm4Util.encryptEcb((String) datas.get("policy_holder".toUpperCase()));
//            this.setPolicy_holder((String) datas.get("policy_holder".toUpperCase()));//加密
            this.setPolicy_holder(Sm4Util.encryptEcb((String) datas.get("policy_holder".toUpperCase())));//加密
//            this.setPolicy_holder_id_card((String) datas.get("policy_holder_id_card".toUpperCase())); //加密
            this.setPolicy_holder_id_card(Sm4Util.encryptEcb((String) datas.get("policy_holder_id_card".toUpperCase()))); //加密


//            this.setAccumulate_premium((String) datas.get("accumulate_premium".toUpperCase()));  //加密
            this.setAccumulate_premium(Sm4Util.encryptEcb((String) datas.get("accumulate_premium".toUpperCase())));  //加密
            BigDecimal bigDecimal = (BigDecimal) datas.get("loan_amount".toUpperCase());
            if (bigDecimal != null) {
                String bigDecimala = Sm4Util.encryptEcb(bigDecimal.toString());
                this.setLoan_amount(bigDecimala); //加密
//                this.setLoan_amount(bigDecimal.toString()); //加密
            }
            String return_amount = (String) datas.get("return_amount".toUpperCase());
            if (return_amount == null) {
                return_amount = "0.00";
            }
            this.setReturn_amount(Sm4Util.encryptEcb(return_amount)); //加密
//            this.setReturn_amount((String) datas.get("return_amount".toUpperCase())); //加密

        }


    }

}

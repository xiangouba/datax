package com.alibaba.datax.plugin.reader.chongqingreader.tool;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @auther: rihang
 * @create: 2022-07-06
 * @Description:  理赔数据
 * ①以领取人身份证号码为查询条件（没有领取人身份证号码时以被保险人身份证号码为查询条件），汇总各保单理赔数据
 * ②零赔付和未决数据不查询
 * ③同一保单下险种很多时，只显示主险种(险种名称同)
 * 表名 claim_settlement_result
 * 参数名 必填 类型范围 说明
 */
@Data
@Slf4j
public class LisLipeiData {


    private String id;                 // 是 String 批次号(请求字段原数据返回即可）
    private String batch_no;                 // 是 String 批次号(请求字段原数据返回即可）
    private String claim_id;                 // 是 String 理赔表主键（32 位 UUID，各保险公司自定义）
    private String commission_id;                // 是 String 记录 ID(请求字段原数据返回即可)
    private String people_id;                // 是 String 人员 ID(请求字段原数据返回即可)
    private String corporate_name ="英大泰和人寿";                                  // 是 String 保险公司名称
    private String result_state ="1";            // 是 String “0”代表查询无结果，“1”查询有结果（无结果时后续字段不必填）
    private String policy_no;                // 是 String 保单号
    private String policy_holder;           // 是 String 投保人姓名           //加密
    private String policy_holder_id_card;           // 是 String 投保人身份证号码
    private String insured_name;            // 是 String 被保人姓名
    private String insured_id_card;         // 是 String 被保人身份证号码
    private String receiver_name;           // 是 String 领取人姓名
    private String receiver_id_card;            // 否 String 领取人身份证号码(以领取人为查询条件时必填)
    private String receiver_phone;          // 否 String 领取人联系电话
    private String insurance_type;          // 是 String 险种（中文险种分类名，以各保险公司险种分类为准）
    private String insurance_name;          // 是 String 保险名称
    private String first_claim_time;            // 是 String 首次理赔时间 YYYY-MM-DD（首选财务打款时间，无打款时间以结案时间为准）
    private String last_claim_time;         // 是 String 最近一次理赔时间 YYYY-MM-DD（没有二次理赔的保单，填首次理赔时间）
    private String accumulate_premium;          // 是 Number 累积理赔金额（元）（按保单号汇总理赔金额，保留两位小数，不含理赔退保金额）
    private String year_premium;            // 是 Number 上年累计理赔金额（查询当日后退 365天累计理赔金额，不含理赔退保金额）




    public void convert_Enaty( String batch_no, String commission_id, String people_id) {
        this.id = StringUtil.getUUid();
        this.batch_no = batch_no;
        this.commission_id = commission_id;
        this.people_id = people_id;
        this.claim_id =  id;
        this.result_state ="0";

    }
        public void convert_Enaty(Map<String, Object> data, String batch_no, String commission_id, String people_id) {
            this.id = StringUtil.getUUid();
            this.batch_no = batch_no;
            this.commission_id = commission_id;
            this.people_id = people_id;
            this.claim_id = (String) data.get("claim_id".toUpperCase());



            this.result_state = (String) data.get("result_state".toUpperCase());
            this.policy_no = (String) data.get("policy_no".toUpperCase());
            this.receiver_phone = (String) data.get("receiver_phone".toUpperCase());
            this.insurance_type = (String) data.get("insurance_type".toUpperCase());
            this.insurance_name = (String) data.get("insurance_name".toUpperCase());
            try {

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date date1 = (Date) data.get("first_claim_time".toUpperCase());
                if (null != date1) {
                    this.first_claim_time = simpleDateFormat.format(date1);
                }
                Date date2 = (Date) data.get("last_claim_time".toUpperCase());
//            (String) data.get("last_claim_time".toUpperCase());
                if (date2 != null) {
                    this.last_claim_time = simpleDateFormat.format(date2);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            String policy_holder = (String) data.get("policy_holder".toUpperCase());           //加密
            this.policy_holder = Sm4Util.encryptEcb(policy_holder);

            String policy_holder_id_card = (String) data.get("policy_holder_id_card".toUpperCase());          //加密
            this.policy_holder_id_card = Sm4Util.encryptEcb(policy_holder_id_card);
            String insured_name = (String) data.get("insured_name".toUpperCase());             //加密
            this.insured_name = Sm4Util.encryptEcb(insured_name);
            String insured_id_card = (String) data.get("insured_id_card".toUpperCase()); //加密
            this.insured_id_card = Sm4Util.encryptEcb(insured_id_card);
            String receiver_name = (String) data.get("receiver_name".toUpperCase()); //加密
            this.receiver_name = Sm4Util.encryptEcb(receiver_name);
            String receiver_id_card = (String) data.get("receiver_id_card".toUpperCase()); //加密
            this.receiver_id_card = Sm4Util.encryptEcb(receiver_id_card);
            String accumulate_premium = (String) data.get("accumulate_premium".toUpperCase());//BigDecimal
            this.accumulate_premium = Sm4Util.encryptEcb(accumulate_premium); //加密
            String year_premium = (String) data.get("year_premium".toUpperCase()); //BigDecimal
            this.year_premium = Sm4Util.encryptEcb(year_premium);
        if (null != claim_id) {

        }else {
            this.result_state ="0";
            this.claim_id= this.id;
        }

    }



}


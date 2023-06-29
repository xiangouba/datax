package com.alibaba.datax.plugin.reader.chongqingreader.tool;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@Data
public class LisChengbao {


    private String id;                                    // 是 String 批次号(请求字段原数据返回即可）
    private String batch_no;                                    // 是 String 批次号(请求字段原数据返回即可）
    private String commission_id;                                   // 是 String 记录 ID(请求字段原数据返回即可)
    private String people_id;                                   // 是 String 人员 ID(请求字段原数据返回即可)
    private String corporate_name ="英大泰和人寿";                                  // 是 String 保险公司名称
    private String result_state ="1";
    // 是 String “0”代表查询无结果，“1”查询有结果（无结果时后续字段不必填）
    private String policy_no;                                   // 是 String 保单号
    private String policy_holder;                                   // 是 String 投保人姓名
    private String policy_holder_id_card;                                   // 是 String 投保人身份证号码
    private String insured_name;                                    // 是 String 被保人姓名
    private String Insured_id_card;                                     // 是 String 被保人身份证号码
    private String insurance_type;                                  // 是 String 险种（中文险种分类名，以各保险公司险种分类为准）
    private String insurance_name;                                  //  否 String 保险名称
    private String first_pay_time;                                  // 是 String 首次缴费时间 YYYY-MM-DD
    private String last_pay_time;                                   // 是 String 最近一次缴费时间 YYYY-MM-DD（期交返回最近一次缴费时间，趸交如果没有批增费用就返首次缴费时间，有批增费就返增费时间）
    private String accumulate_premium;                                  // 是 Number 累交保费（元）（按保单号汇总保费，保留两位小数）
    private String surrender_value;                                     // 否 Number 退保金额（元）（按保单号汇总退保金额，保留两位小数）
    private String insurance_status;                                    // 是 String 保险状态（失效、终止、退保等，以各保险公司设定保险状态为准）
    private String pay_method;                                  // 是 String 缴费方式（年交不需要具体到多少年）
    private String year_premium;                                    // 是 Number 上年缴费总金额（查询当日后退 365 天累交保费）
    private String start_time;                                  // 是 String 主险生效时间 YYYY-MM-DD


    public void convert_Enaty(  String batch_no, String commission_id, String people_id) {
        this.id = StringUtil.getUUid();
        //-- 是 String 批次号(请求字段原数据返回即可）
        this.batch_no  = batch_no   ;
        //-- 是 String 记录 ID(请求字段原数据返回即可)
        this.commission_id  = commission_id   ;
        //-- 是 String 人员 ID(请求字段原数据返回即可)
        this.people_id  = people_id   ;
        //-- 是 String 保险公司名称
        this.corporate_name  = "英大泰和人寿"   ;
        //-- 是 String “0”代表查询无结果，“1”查询有结果是 String “0”代表查询无结果，“1”查询有结果
        this.result_state  =  "0" ;
    }

    public void convert_Enaty(Map<String, Object> datas, String batch_no, String commission_id, String people_id) {
        this.id = StringUtil.getUUid();
        this.batch_no = batch_no;
        this.commission_id = commission_id;
        this.people_id = people_id;
        this.policy_no = (String) datas.get("policy_no".toUpperCase());

        this.insurance_type = (String) datas.get("insurance_type".toUpperCase());
        this.insurance_name = (String) datas.get("insurance_name".toUpperCase());


        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date1 = (Date) datas.get("first_pay_time".toUpperCase());

            if (null != date1) {
                this.first_pay_time = simpleDateFormat.format(date1);
            }

            Date date2 = (Date) datas.get("last_pay_time".toUpperCase());
            if (date2 != null) {
                this.last_pay_time = simpleDateFormat.format(date2);
            }

            Date date3 = (Date) datas.get("start_time".toUpperCase());
            if (date3 != null ) {
                this.start_time = simpleDateFormat.format(date3);
            }

        } catch (Exception e) {

        }

        this.insurance_status = (String) datas.get("insurance_status".toUpperCase());
        String pay_method = (String) datas.get("pay_method".toUpperCase());
        if (pay_method!=null){
            this.pay_method =pay_method.trim() ;
        }

//加密this.
        String policy_holder = (String) datas.get("policy_holder".toUpperCase());
        this.policy_holder =  Sm4Util.encryptEcb(policy_holder);
        String policy_holder_id_card = (String) datas.get("policy_holder_id_card".toUpperCase());
        this.policy_holder_id_card = Sm4Util.encryptEcb(policy_holder_id_card);
        String insured_name = (String) datas.get("insured_name".toUpperCase());
        this.insured_name = Sm4Util.encryptEcb(insured_name);
        String Insured_id_card = (String) datas.get("Insured_id_card".toUpperCase());
        this.Insured_id_card = Sm4Util.encryptEcb(Insured_id_card);

        String  accumulate_premium   =    (String ) datas.get("accumulate_premium".toUpperCase());
        if (null != accumulate_premium) {
            this.accumulate_premium = Sm4Util.encryptEcb(accumulate_premium);
        }else {
            this.accumulate_premium = Sm4Util.encryptEcb("0.00");

        }

        String surrender_value   = (String) datas.get("surrender_value".toUpperCase());
        if (null != surrender_value) {
            this.surrender_value = Sm4Util.encryptEcb(surrender_value);
        }else {
            this.surrender_value = Sm4Util.encryptEcb("0.00");
        }

        String year_premium= (String) datas.get("year_premium".toUpperCase());
        if (null != year_premium) {
            this.year_premium =Sm4Util.encryptEcb(year_premium);
        }
    }


}

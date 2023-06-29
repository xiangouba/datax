package com.alibaba.datax.plugin.writer.sjsqwriter.entity;


import com.alibaba.datax.plugin.writer.sjsqwriter.util.MD5Util;

import java.util.Date;


/**
 * 承保契调信息
 *
 */
public class RsUnderwritingContractInfo {

    private String id;
    //单位名称
    private String company_name;
    //单位编号
    private String company_id;
    //////    //投保险种
    //////   private String insurance_type;
    //保单号
    private String policy_num;
    //////    //投保人
    //////    private String policy_holder;
    //契调下发时间
    private Date contract_delivery_time;
    //契调回收时间
    private Date contract_recovery_time;
    //承保时间
    private Date underwriting_time;
    //投保单号
    private String policy_number2;
    //系统版本
    private String g_version;
    //抽数时间
    private Date g_create_time;


    public RsUnderwritingContractInfo() {

    }

    public RsUnderwritingContractInfo(String company_name, String company_id, String policy_num,
                                      Date contract_delivery_time, Date contract_recovery_time,
                                      Date underwriting_time, String policy_number2, String g_version, Date g_create_time) {
//        this.id = id;
        this.company_name = company_name;
        this.company_id = company_id;
//        this.insurance_type = insurance_type;
        this.policy_num = policy_num;
//        this.policy_holder = policy_holder;
        this.contract_delivery_time = contract_delivery_time;
        this.contract_recovery_time = contract_recovery_time;
        this.underwriting_time = underwriting_time;
        this.policy_number2 = policy_number2;
        this.g_version = g_version;
        this.g_create_time = g_create_time;

        this.id = MD5Util.encrypt(this.toString());
    }


    @Override
    public String toString() {
        return "RsUnderwritingContractInfo{" +
//                "id='" + id + '\'' +", "
                "company_name='" + company_name + '\'' +
                ", company_id='" + company_id + '\'' +
                ", policy_num='" + policy_num + '\'' +
                ", contract_delivery_time=" + contract_delivery_time +
                ", contract_recovery_time=" + contract_recovery_time +
                ", underwriting_time=" + underwriting_time +
                ", policy_number2='" + policy_number2 + '\'' +
                ", g_version='" + g_version + '\'' +
//                ", g_create_time=" + g_create_time +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCompany_name() {
        return company_name;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public String getCompany_id() {
        return company_id;
    }

    public void setCompany_id(String company_id) {
        this.company_id = company_id;
    }

    public String getPolicy_num() {
        return policy_num;
    }

    public void setPolicy_num(String policy_num) {
        this.policy_num = policy_num;
    }

    public Date getContract_delivery_time() {
        return contract_delivery_time;
    }

    public void setContract_delivery_time(Date contract_delivery_time) {
        this.contract_delivery_time = contract_delivery_time;
    }

    public Date getContract_recovery_time() {
        return contract_recovery_time;
    }

    public void setContract_recovery_time(Date contract_recovery_time) {
        this.contract_recovery_time = contract_recovery_time;
    }

    public Date getUnderwriting_time() {
        return underwriting_time;
    }

    public void setUnderwriting_time(Date underwriting_time) {
        this.underwriting_time = underwriting_time;
    }

    public String getPolicy_number2() {
        return policy_number2;
    }

    public void setPolicy_number2(String policy_number2) {
        this.policy_number2 = policy_number2;
    }

    public String getG_version() {
        return g_version;
    }

    public void setG_version(String g_version) {
        this.g_version = g_version;
    }

    public Date getG_create_time() {
        return g_create_time;
    }

    public void setG_create_time(Date g_create_time) {
        this.g_create_time = g_create_time;
    }
}

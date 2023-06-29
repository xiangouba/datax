package com.alibaba.datax.plugin.writer.sjsqwriter.entity;


import com.alibaba.datax.plugin.writer.sjsqwriter.util.MD5Util;

import java.util.Date;

/**
 * 人寿承保信息表
 *
 */
public class RsUnderwritingInfo {
    private String id;
    //单位名称
    private String company_name;
    //单位编号
    private String company_id;
    //保单号
    private String policy_num;
    //保单签字日期（投保日期）
    private Date policy_signing_date;
    //回执日期（客户签收日期）
    private Date receipt_date;
    //承保时间
    private Date underwriting_time;
    //投保单号
    private String policy_number2;
    //业务员编码
    private String salesman_id;

    //系统版本
    private String g_version;

    //抽数时间
    private Date g_create_time;

    public RsUnderwritingInfo() {
    }

    public RsUnderwritingInfo(String company_name, String company_id,
                              String policy_num, Date policy_signing_date, Date receipt_date,
                              Date underwriting_time, String policy_number2, String salesman_id,
                              String g_version, Date g_create_time) {

        this.company_name = company_name;
        this.company_id = company_id;
        this.policy_num = policy_num;
        this.policy_signing_date = policy_signing_date;
        this.receipt_date = receipt_date;
        this.underwriting_time = underwriting_time;
        this.policy_number2 = policy_number2;
        this.salesman_id = salesman_id;
        this.g_version = g_version;
        this.g_create_time = g_create_time;

        this.id = MD5Util.encrypt(this.toString());
    }

    @Override
    public String toString() {
        return "RsUnderwritingInfo{" +
                "company_name='" + company_name + '\'' +
                ", company_id='" + company_id + '\'' +
                ", policy_num='" + policy_num + '\'' +
                ", policy_signing_date=" + policy_signing_date +
                ", receipt_date=" + receipt_date +
                ", underwriting_time=" + underwriting_time +
                ", policy_number2='" + policy_number2 + '\'' +
                ", salesman_id='" + salesman_id + '\'' +
                ", g_version='" + g_version + '\'' +
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

    public Date getPolicy_signing_date() {
        return policy_signing_date;
    }

    public void setPolicy_signing_date(Date policy_signing_date) {
        this.policy_signing_date = policy_signing_date;
    }

    public Date getReceipt_date() {
        return receipt_date;
    }

    public void setReceipt_date(Date receipt_date) {
        this.receipt_date = receipt_date;
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

    public String getSalesman_id() {
        return salesman_id;
    }

    public void setSalesman_id(String salesman_id) {
        this.salesman_id = salesman_id;
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

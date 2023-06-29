package com.alibaba.datax.plugin.writer.sjsqwriter.entity;

import com.alibaba.datax.plugin.writer.sjsqwriter.util.MD5Util;

import java.util.Date;

/**
 * 业务员信息
 *
 * @author cy
 * @date 2021/07/21 17:13
 */
public class RsSalesmanInfo {

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSalesman() {
        return salesman;
    }

    public void setSalesman(String salesman) {
        this.salesman = salesman;
    }

    public String getBusiness_license_no() {
        return business_license_no;
    }

    public void setBusiness_license_no(String business_license_no) {
        this.business_license_no = business_license_no;
    }

    public Date getLicense_valid_start_date() {
        return license_valid_start_date;
    }

    public void setLicense_valid_start_date(Date license_valid_start_date) {
        this.license_valid_start_date = license_valid_start_date;
    }

    public Date getLicense_expiration_date() {
        return license_expiration_date;
    }

    public void setLicense_expiration_date(Date license_expiration_date) {
        this.license_expiration_date = license_expiration_date;
    }

    public String getSalesman_id() {
        return salesman_id;
    }

    public void setSalesman_id(String salesman_id) {
        this.salesman_id = salesman_id;
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

    public String getPolicy_number2() {
        return policy_number2;
    }

    public void setPolicy_number2(String policy_number2) {
        this.policy_number2 = policy_number2;
    }

    public String getComptxt() {
        return comptxt;
    }

    public void setComptxt(String comptxt) {
        this.comptxt = comptxt;
    }

    public String getCompid() {
        return compid;
    }

    public void setCompid(String compid) {
        this.compid = compid;
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

    private String id;
    /**
     * 业务员名称
     */
    private String salesman;
    /**
     * 业务员执业证编号
     */
    private String business_license_no;
    /**
     * 业务员执业证起始日期
     */
    private Date license_valid_start_date;
    /**
     * 执业证有效截止日期
     */
    private Date license_expiration_date;
    /**
     * 业务员编码
     */
    private String salesman_id;
    /**
     * 保单号
     */
    private String policy_num;
    /**
     * 投保日期
     */
    private Date policy_signing_date;

    /**
     * 投保单号
     */
    private String policy_number2;
    /**
     * 单位名称
     */
    private String comptxt;
    /**
     * 单位编码
     */
    private String compid;
    /**
     * 系统版本
     */
    private String g_version;
    /**
     * 抽数时间
     */
    private Date g_create_time;

    public RsSalesmanInfo() {

    }

    public RsSalesmanInfo(String salesman, String business_license_no, Date license_valid_start_date, Date license_expiration_date,
                          String salesman_id, String policy_num, Date policy_signing_date,
                          String policy_number2, String compid, String comptxt, String g_version, Date g_create_time) {
        this.salesman = salesman;
        this.business_license_no = business_license_no;
        this.license_valid_start_date = license_valid_start_date;
        this.license_expiration_date = license_expiration_date;
        this.salesman_id = salesman_id;
        this.policy_num = policy_num;
        this.policy_number2 = policy_number2;
        this.policy_signing_date = policy_signing_date;
        this.compid = compid;
        this.comptxt = comptxt;
        this.g_version = g_version;
        this.g_create_time = g_create_time;

        this.id = MD5Util.encrypt(this.toString());
    }

//    @Override
//    public String toString() {
////        StringBuilder str = new StringBuilder();
////        str.append("RsSalesmanInfo{salesman='").append(salesman).append("',business_license_no='")
////                .append(business_license_no).append("', license_valid_start_date='").append(license_valid_start_date).append("', license_expiration_date='").append(license_expiration_date).append("', salesman_id='").append(salesman_id).append("', policy_num='")
////                .append(policy_num).append("', policy_signing_date=").append(policy_signing_date).append("', policy_number2='").append(policy_number2).append("', comptxt='").append(comptxt).append("', compid='").append(compid).append("', g_version='").append(g_version).append("'}");
////        return "" +
//////                "id='" + id + '\'' +", "
////                "" +  + '\'' +
////                "," +  + '\'' +
////                "" +  +
////                "" +  +
////                "" +  + '\'' +
////                "" +  + '\'' +
////                "" +  +
////                "" +  + '\'' +
////                "" +  + '\'' +
////                "" +  + '\'' +
////                "" +  + '\'' +
//////                ", g_create_time=" + g_create_time +
////                '}';
////        return str.toString();
//    }

    @Override
    public String toString() {
        return "RsSalesmanInfo{" +
//                "id='" + id + '\'' +", "
                "salesman='" + salesman + '\'' +
                ", business_license_no='" + business_license_no + '\'' +
                ", license_valid_start_date=" + license_valid_start_date +
                ", license_expiration_date=" + license_expiration_date +
                ", salesman_id='" + salesman_id + '\'' +
                ", policy_num='" + policy_num + '\'' +
                ", policy_signing_date=" + policy_signing_date +
                ", policy_number2='" + policy_number2 + '\'' +
                ", comptxt='" + comptxt + '\'' +
                ", compid='" + compid + '\'' +
                ", g_version='" + g_version + '\'' +
//                ", g_create_time=" + g_create_time +
                '}';
    }
}

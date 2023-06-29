package com.alibaba.datax.plugin.writer.sjsqwriter.entity;

import com.alibaba.datax.plugin.writer.sjsqwriter.util.MD5Util;

import java.util.Date;


/**
 * 人寿理赔信息表
 *
 * @author cy
 * @date 2021/07/21 17:13
 */
public class RsClaimInfo {

    private String id;
    //单位名称
    private String company_name;
    //单位编码
    private String company_id;
    //保单号
    private String policy_num;
    //////     实际赔付金额
//////     private BigDecimal claim_amount;
    //理赔支付时间
    private Date claim_settlement_time;
    //索赔资料接收齐全日期
    private Date filing_time;
    //调查
    private String investigation;
    //理赔类型
    private String claim_type;
    //赔案号
    private String otherno;

    private String g_version;

    private Date g_create_time;


    public RsClaimInfo(String company_name, String company_id, String policy_num,
//                       BigDecimal claim_amount,
                       Date claim_settlement_time, Date filing_time, String investigation,
                       String claim_type, String otherno, String g_version, Date g_create_time) {
//        this.id = id;
        this.company_name = company_name;
        this.company_id = company_id;
        this.policy_num = policy_num;
//        this.claim_amount = claim_amount;
        this.claim_settlement_time = claim_settlement_time;
        this.filing_time = filing_time;
        this.investigation = investigation;
        this.claim_type = claim_type;
        this.otherno = otherno;
        this.g_version = g_version;
        this.g_create_time = g_create_time;
        this.id = MD5Util.encrypt(this.toString());
    }

    @Override
    public String toString() {
        return "RsClaimInfo{" +
//                "id='" + id + '\'' +", "
                "company_name='" + company_name + '\'' +
                ", company_id='" + company_id + '\'' +
                ", policy_num='" + policy_num + '\'' +
                ", claim_settlement_time=" + claim_settlement_time +
                ", filing_time=" + filing_time +
                ", investigation='" + investigation + '\'' +
                ", claim_type='" + claim_type + '\'' +
                ", otherno='" + otherno + '\'' +
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

    public Date getClaim_settlement_time() {
        return claim_settlement_time;
    }

    public void setClaim_settlement_time(Date claim_settlement_time) {
        this.claim_settlement_time = claim_settlement_time;
    }

    public Date getFiling_time() {
        return filing_time;
    }

    public void setFiling_time(Date filing_time) {
        this.filing_time = filing_time;
    }

    public String getInvestigation() {
        return investigation;
    }

    public void setInvestigation(String investigation) {
        this.investigation = investigation;
    }

    public String getClaim_type() {
        return claim_type;
    }

    public void setClaim_type(String claim_type) {
        this.claim_type = claim_type;
    }

    public String getOtherno() {
        return otherno;
    }

    public void setOtherno(String otherno) {
        this.otherno = otherno;
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

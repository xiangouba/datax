package com.alibaba.datax.plugin.writer.sjsqwriter.util;


import com.alibaba.datax.plugin.writer.sjsqwriter.entity.*;

import java.util.*;

/**
 * 工具类
 *
 * @author cy
 * @date 2021/08/06 10:56
 **/
public class DataUtile {

    //    public static String table_rs_salesman_info = "rs_salesman_info";
//    public static String table_rs_claim_info = "rs_claim_info";
//    public static String table_rs_underwriting_contract_info = "rs_underwriting_contract_info";
//    public static String table_rs_underwriting_info = "rs_underwriting_info";
    public static String table_rs_salesman_info = "qryj_z_rs_salesman_info";
    public static String table_rs_claim_info = "qryj_z_rs_claim_info";
    public static String table_rs_underwriting_contract_info = "qryj_z_rs_un_contract_info";
    public static String table_rs_underwriting_info = "qryj_z_rs_underwriting_info";


    public static DeltaPair receiveDefectList(Map<String, List<String>> oldMap, Map<String, List<RsClaimInfo>> newMap) {
        List<String> firstDeltaList = new LinkedList<String>();

        Set<String> secondSetDelta = new HashSet<String>(newMap.keySet());

        for (String id : oldMap.keySet()) {
            if (secondSetDelta.contains(id)) {
                if (oldMap.get(id).size() == newMap.get(id).size()) {
                    secondSetDelta.remove(id);
                } else {
                    firstDeltaList.add(id);
                }
            } else {
                firstDeltaList.add(id);
            }
        }

        return new DeltaPair(firstDeltaList, secondSetDelta);
    }

    public static DeltaPair receiveDefectList_1(Map<String, List<String>> oldMap, Map<String, List<RsUnderwritingContractInfo>> newMap) {
        List<String> firstDeltaList = new LinkedList<String>();

        Set<String> secondSetDelta = new HashSet<String>(newMap.keySet());

        for (String id : oldMap.keySet()) {
            if (secondSetDelta.contains(id)) {
                if (oldMap.get(id).size() == newMap.get(id).size()) {
                    secondSetDelta.remove(id);
                } else {
                    firstDeltaList.add(id);
                }
            } else {
                firstDeltaList.add(id);
            }
        }

        return new DeltaPair(firstDeltaList, secondSetDelta);
    }

    public static DeltaPair receiveDefectList_2(Map<String, List<String>> oldMap, Map<String, List<RsSalesmanInfo>> newMap) {

        //需要删除的数据
        List<String> firstDeltaList = new LinkedList<String>();

        //需要新增的数据
        Set<String> secondSetDelta = new HashSet<String>(newMap.keySet());

        for (String id : oldMap.keySet()) {
            if (secondSetDelta.contains(id)) {
                if (oldMap.get(id).size() == newMap.get(id).size()) {
                    secondSetDelta.remove(id);
                } else {
                    firstDeltaList.add(id);
                }
            } else {
                firstDeltaList.add(id);
            }
        }

        return new DeltaPair(firstDeltaList, secondSetDelta);
    }


    public static DeltaPair receiveDefectList_3(Map<String, List<String>> oldMap, Map<String, List<RsUnderwritingInfo>> newMap) {
        List<String> firstDeltaList = new LinkedList<String>();

        Set<String> secondSetDelta = new HashSet<String>(newMap.keySet());

        for (String id : oldMap.keySet()) {
            if (secondSetDelta.contains(id)) {
                if (oldMap.get(id).size() == newMap.get(id).size()) {
                    secondSetDelta.remove(id);
                } else {
                    firstDeltaList.add(id);
                }
            } else {
                firstDeltaList.add(id);
            }
        }

        return new DeltaPair(firstDeltaList, secondSetDelta);
    }

    public static DeltaPair receiveDefectList(List<String> firstArrayList, Set<String> secondArrayList) {
        List<String> firstDeltaList = new LinkedList<String>();

        Set<String> secondSetDelta = new HashSet<String>(secondArrayList);

        for (String id : firstArrayList) {
            if (secondSetDelta.contains(id)) {
                secondSetDelta.remove(id);
            } else {
                firstDeltaList.add(id);
            }
        }

        return new DeltaPair(firstDeltaList, secondSetDelta);

    }

    public static List<RsRecord> getRsRecordList(String tableName, List<String> ids, String opt) {
        List<RsRecord> recordList = new ArrayList<>();
        for (String id : ids) {
            RsRecord record = new RsRecord();
            record.setOpt(opt);
            record.setMd5code(id);
            record.setTable_name(tableName);
            recordList.add(record);
        }
        return recordList;
    }

    public static List<RsRecord> getRsRecordSet(String tableName, Set<String> ids, String opt) {
        List<RsRecord> recordList = new ArrayList<>();
        for (String id : ids) {
            RsRecord record = new RsRecord();
            record.setOpt(opt);
            record.setMd5code(id);
            record.setTable_name(tableName);
            recordList.add(record);
        }
        return recordList;
    }


}

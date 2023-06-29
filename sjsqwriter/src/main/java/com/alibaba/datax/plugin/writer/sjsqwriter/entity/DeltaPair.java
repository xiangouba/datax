package com.alibaba.datax.plugin.writer.sjsqwriter.entity;


import java.util.List;
import java.util.Set;

public class DeltaPair {

    private List<String> delete;
    private Set<String> insert;

    public DeltaPair(List<String> delete, Set<String> insert) {
        this.delete = delete;
        this.insert = insert;
    }

    public List<String> getDelete() {
        return delete;
    }

    public Set<String> getInsert() {
        return insert;
    }
}

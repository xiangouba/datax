package com.alibaba.datax.plugin.writer.sjsqwriter.entity;



/**
 * 测试类
 *
 * @author cy
 * @date 2021/07/21 17:13
 **/
public class Content {

    private String id;

    private String content;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Content(String id, String content) {
        this.id = id;
        this.content = content;
    }

    public Content() {
    }
}

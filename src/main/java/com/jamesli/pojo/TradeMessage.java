package com.jamesli.pojo;

import java.util.List;

public class TradeMessage {
    private List<Trade> data;
    private String type;

    public List<Trade> getData() {
        return data;
    }

    public void setData(List<Trade> data) {
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

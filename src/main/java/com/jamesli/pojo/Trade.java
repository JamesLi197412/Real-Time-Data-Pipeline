package com.jamesli.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class Trade {
    private Object c;   // List of trade conditions

    @JsonProperty("p")
    private double price;

    @JsonProperty("s")
    private String symbol;

    @JsonProperty("t")
    private long timestamp;

    @JsonProperty("v")
    private double volumne;

    public Object getC() { return c;}

    public void setC(Object c) {
        this.c = c;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getVolumne() {
        return volumne;
    }

    public void setVolumne(double volumne) {
        this.volumne = volumne;
    }
}

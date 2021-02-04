package com.amazonaws.samples.ex.ml.events;

public class TradeData {
    private String ticker;
    private double price;
    private long eventtime;

    public TradeData(String ticker, double price, long eventtime) {
        this.ticker = ticker;
        this.price = price;
        this.eventtime = eventtime;
    }

    public String getTicker() {
        return ticker;
    }

    public TradeData() {
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getEventtime() {
        return eventtime;
    }

    public void setEventtime(long eventtime) {
        this.eventtime = eventtime;
    }

    @Override
    public String toString() {
        return String.valueOf(this.price);
    }
}

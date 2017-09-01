package io.confluent.examples.streams.pojo;

import java.io.Serializable;

public class PageViewStats implements Serializable {

    private String user;
    private String page;
    private String industry;
    private long count;

    public PageViewStats(String user, String page, String industry, long count) {
        this.user = user;
        this.page = page;
        this.industry = industry;
        this.count = count;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewStats{" +
                "user='" + user + '\'' +
                ", page='" + page + '\'' +
                ", industry='" + industry + '\'' +
                ", count=" + count +
                '}';
    }
}

package my.examples.streams.pojo;

import java.io.Serializable;

public class PageView implements Serializable {

    private String user;
    private String page;
    private String industry;
    private String flags;

    public PageView() {
    }

    public PageView(String user, String page, String industry, String flags) {
        this.user = user;
        this.page = page;
        this.industry = industry;
        this.flags = flags;
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

    public String getFlags() {
        return flags;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    @Override
    public String toString() {
        return "PageView{" +
                "user='" + user + '\'' +
                ", page='" + page + '\'' +
                ", industry='" + industry + '\'' +
                ", flags='" + flags + '\'' +
                '}';
    }
}

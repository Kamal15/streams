package my.examples.streams.pojo;

import java.io.Serializable;

public class PageViewRegion implements Serializable {

    private String user;
    private String page;
    private String region;

    public PageViewRegion(String user, String page, String region) {
        this.user = user;
        this.page = page;
        this.region = region;
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

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "PageViewRegion{" +
                "user='" + user + '\'' +
                ", page='" + page + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}

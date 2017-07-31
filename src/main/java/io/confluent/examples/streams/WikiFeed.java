package io.confluent.examples.streams;

import java.io.Serializable;

public class WikiFeed implements Serializable {

    private static final long serialVersionUID = 1;

    private String user;
    private boolean isNew;
    private String content;

    public WikiFeed() {
    }

    public WikiFeed(String user, boolean isNew, String content) {
        this.user = user;
        this.isNew = isNew;
        this.content = content;
    }

    public String getUser() {
        return user;
    }

    public boolean isNew() {
        return isNew;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "WikiFeed{" +
                "user='" + user + '\'' +
                ", isNew=" + isNew +
                ", content='" + content + '\'' +
                '}';
    }
}

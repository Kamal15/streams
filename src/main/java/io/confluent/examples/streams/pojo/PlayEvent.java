package io.confluent.examples.streams.pojo;

import java.io.Serializable;

public class PlayEvent implements Serializable {

    private String name;
    private int age;

    public PlayEvent(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "PlayEvent{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                '}';
    }
}
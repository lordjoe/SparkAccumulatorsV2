package com.lordjoe.testing;

import java.io.Serializable;

/**
 * This is a Java object which is not a bean
 * no getters or setters but is serializable
 */
public class MyUnBean implements Serializable {
    public final int count;
    public final String name;

    public MyUnBean(int count, String name) {
        this.count = count;
        this.name = name;
    }
}

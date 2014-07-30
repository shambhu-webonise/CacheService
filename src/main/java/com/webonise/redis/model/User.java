package com.webonise.redis.model;

import java.io.Serializable;

public class User implements Serializable {

    private static final long serialVersionUID = -7898194272883238670L;
    private static final String OBJECT_KEY = "USER";
    
    private String id;
    private String name;
    
    public User() {
        super();
    }
    
    public User(String id) {
        super();
        this.id = id;
    }

    public User(String id, String name) {
        super();
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getKey() {
        return getId();
    }
    public String getObjectKey() {
        return OBJECT_KEY;
    }
    @Override
    public String toString() {
        return "User [id=" + id + ", name=" + name + "]";
    }
    
    
}

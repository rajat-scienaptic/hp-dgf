package com.hp.dgf.model;

public class User {
    String name;
    String email;
    String authenticated;

    public User() {
    }

    public User(String name, String email, String authenticated) {
        this.name = name;
        this.email = email;
        this.authenticated = authenticated;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(String authenticated) {
        this.authenticated = authenticated;
    }
}

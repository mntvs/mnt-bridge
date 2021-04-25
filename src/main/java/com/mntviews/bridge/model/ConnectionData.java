package com.mntviews.bridge.model;

import lombok.Data;

@Data
public class ConnectionData {

    public ConnectionData(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    String url;
    String userName;
    String password;
}

package com.mntviews.bridge.model;

import lombok.Data;

@Data
public class ConnectionData {

    public ConnectionData(String url, String userName, String password, String schemaName) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schemaName = schemaName;
    }

    String url;
    String userName;
    String password;
    String schemaName;
}

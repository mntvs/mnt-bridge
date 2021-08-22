package com.mntviews.bridge.model;

import com.mntviews.bridge.service.BridgeContext;
import lombok.Data;

@Data
public class ConnectionData {

    public ConnectionData(String url, String userName, String password, String schemaName) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schemaName = schemaName;
    }

    public ConnectionData(String url, String userName, String password) {
        this(url, userName, password, null);
    }


    final String url;
    final String userName;
    final String password;
    final String schemaName;
}

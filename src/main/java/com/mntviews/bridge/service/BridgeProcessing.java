package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ProcessData;

import java.sql.Connection;

public interface BridgeProcessing {
    public void process(Connection connection,ProcessData processData);
}

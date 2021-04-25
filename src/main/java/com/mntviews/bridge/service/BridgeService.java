package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;

public interface BridgeService {

    public void execute(String groupTag, String metaTag, ConnectionData connectionData, BridgeProcessing process);
}

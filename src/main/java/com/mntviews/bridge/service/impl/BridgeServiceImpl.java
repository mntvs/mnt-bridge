package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.service.BridgeProcessing;
import com.mntviews.bridge.service.BridgeService;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BridgeServiceImpl implements BridgeService {

    private final RawLoopRepo rawLoopRepo;

    @Override
    public void execute(String groupTag, String metaTag, ConnectionData connectionData, BridgeProcessing process) {


        rawLoopRepo.rawLoop();
    }
}

package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.RawData;

import java.sql.Connection;

public interface RawRepo {
    RawData saveRawData(Connection connection, String rawFullName, RawData rawData);

    RawData findRawDataById(Connection connection, String rawFullName, Long id);
}

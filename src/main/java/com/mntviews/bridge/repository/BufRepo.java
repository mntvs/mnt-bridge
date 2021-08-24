package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.RawData;

import java.sql.Connection;

public interface BufRepo {
    BufData findBufDataById(Connection connection, String bufFullName, Long id);
    BufData findBufDataByRawId(Connection connection, String bufFullName, Long id);
}

package com.mntviews.bridge.service;

public interface Bridge {
    void init();
    void clear();

    void migrate(Boolean isClean);
    void migrate();

    void execute();
    void executeOne(Long rawId);
    void executeGroup(String groupId);

}

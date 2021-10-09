package com.mntviews.bridge.model;

import lombok.Data;

@Data
public class ProcessData {
    Long rawId;
    Long bufId;
    MetaData metaData;
    Integer processedStatus;
    String errorMessage;
}

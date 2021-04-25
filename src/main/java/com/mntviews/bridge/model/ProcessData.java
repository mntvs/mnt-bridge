package com.mntviews.bridge.model;

import lombok.Data;

@Data
public class ProcessData {
    Long rawId;
    MetaData metaData;
    Integer processedStatus;
    String errorMessage;
}

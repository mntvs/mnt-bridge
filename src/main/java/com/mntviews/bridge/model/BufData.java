package com.mntviews.bridge.model;

import lombok.Data;

import java.time.OffsetDateTime;

@Data
public class BufData {
    long id;
    String fId;
    OffsetDateTime fDate;
    OffsetDateTime sDate;
    byte fOper;
    String fPayload;
    String sPayload;
    int sCounter;
    long fRawId;
}

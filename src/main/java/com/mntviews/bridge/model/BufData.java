package com.mntviews.bridge.model;

import lombok.Data;

import java.util.Date;

@Data
public class BufData {
    long id;
    byte fOper;
    String fPayload;
    String sPayload;
    Date fDate;
    Date sDate;
    long fRawId;
    String fId;
    int sCounter;
}

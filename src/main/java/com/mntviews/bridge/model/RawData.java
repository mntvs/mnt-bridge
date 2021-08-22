package com.mntviews.bridge.model;

import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;

@Data
public class RawData {
    BigDecimal id;
    String fId;
    OffsetDateTime fDate;
    OffsetDateTime sDate;
    byte fOper;
    String fPayload;
    String fMsg;
    String sMsg;
    byte sStatus;
    byte sAction;
    int sCounter;
}

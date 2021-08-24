package com.mntviews.bridge.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class MetaData {
    BigDecimal groupId;
    BigDecimal metaId;
    String schemaName;
    String rawName;
    String bufName;
    String rawFullName;
    String bufFullName;
    String prcExecName;
    String prcExecFullName;
    String rawLoopQuery;
}

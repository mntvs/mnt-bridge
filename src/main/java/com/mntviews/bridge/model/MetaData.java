package com.mntviews.bridge.model;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

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
    Map<String,Object> param;
    String paramType;
}

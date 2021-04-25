package com.mntviews.bridge.model;

import lombok.Data;

@Data
public class MetaData {
    Long groupId;
    Long metaId;
    String schemaName;
    String rawName;
    String bufName;
    String rawFullName;
    String bufFullName;
    String prcExecName;
    String prcExecFullName;
    String rawLoopQuery;
}

package com.distributekv.kvstore.hintedHandOff.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Hint {
    private String key;
    private String value;
    private String targetNode;   // e.g., http://localhost:8083
    private long timestamp;
    private int retryCount;
}


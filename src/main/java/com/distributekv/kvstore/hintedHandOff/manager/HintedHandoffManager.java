package com.distributekv.kvstore.hintedHandOff.manager;

import com.distributekv.kvstore.hintedHandOff.model.Hint;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class HintedHandoffManager {
    // key = nodeId of down node, value = list of hints to send later
    private final ConcurrentMap<String, CopyOnWriteArrayList<Hint>> hints = new ConcurrentHashMap<>();

    public void storeHint(String targetNode, String key, String value) {
        Hint hint = new Hint(key, value, targetNode, System.currentTimeMillis(), 0);
        hints.computeIfAbsent(targetNode, n -> new CopyOnWriteArrayList<>()).add(hint);
        System.out.printf("Stored hint for %s (key=%s)%n", targetNode, key);
    }

    public ConcurrentMap<String, CopyOnWriteArrayList<Hint>> getAllHints() {
        return hints;
    }

    public void removeHint(String targetNode, Hint hint) {
        hints.getOrDefault(targetNode, new CopyOnWriteArrayList<>()).remove(hint);
    }
}

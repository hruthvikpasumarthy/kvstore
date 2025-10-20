package com.distributekv.kvstore.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class ConsistentHashRing {

    private final SortedMap<Integer, String> ring = new TreeMap<>();
    private final int virtualNodes = 100;

    public ConsistentHashRing(List<String> nodes) {
        for (String node : nodes) {
            for (int i = 0; i < virtualNodes; i++) {
                int hash = hash(node + "#" + i);
                ring.put(hash, node);
            }
        }
    }

    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return ((digest[0] & 0xFF) << 24) |
                    ((digest[1] & 0xFF) << 16) |
                    ((digest[2] & 0xFF) << 8) |
                    (digest[3] & 0xFF);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getNodesForKey(String key, int replicationFactor) {
        if (ring.isEmpty()) return List.of();

        int hash = hash(key);
        List<String> nodes = new ArrayList<>();

        // Start from the first node after key hash
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);
        Iterator<String> it = tailMap.values().iterator();

        while (nodes.size() < replicationFactor && it.hasNext()) {
            String node = it.next();
            if (!nodes.contains(node)) nodes.add(node);
        }

        // Wrap around ring if needed
        if (nodes.size() < replicationFactor) {
            for (String node : ring.values()) {
                if (!nodes.contains(node)) nodes.add(node);
                if (nodes.size() == replicationFactor) break;
            }
        }

        return nodes;
    }


    public Set<String> getAllNodes() {
        return new HashSet<>(ring.values());
    }
}

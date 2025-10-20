package com.distributekv.kvstore.service;

import com.distributekv.kvstore.cluster.ClusterManager;
import com.distributekv.kvstore.db.RocksDBManager;
import com.distributekv.kvstore.filter.BloomFilterManager;
import com.distributekv.kvstore.hintedHandOff.manager.HintedHandoffManager;
import jakarta.annotation.PostConstruct;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.stereotype.Service;
import com.distributekv.kvstore.cache.CacheManager;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KVStoreService {

    private final RocksDBManager rocksDBManager;
    private final CacheManager cacheManager;
    private final BloomFilterManager bloomFilterManager;
    private final ClusterManager clusterManager;
    private final RestTemplate restTemplate = new RestTemplate();
    private final HintedHandoffManager hintedHandoffManager;


    public KVStoreService(RocksDBManager rocksDBManager, CacheManager cacheManager, BloomFilterManager bloomFilterManager, ClusterManager clusterManager, HintedHandoffManager hintedHandoffManager) {
        this.rocksDBManager = rocksDBManager;
        this.cacheManager = cacheManager;
        this.bloomFilterManager = bloomFilterManager;
        this.clusterManager = clusterManager;
        this.hintedHandoffManager = hintedHandoffManager;
    }

    @PostConstruct
    public void rebuildBloomFilter() {
        List<String> allKeys = rocksDBManager.getAllKeys();
        for (String key : allKeys) {
            bloomFilterManager.add(key);
        }
    }

    public boolean put(String key, String value) {
        List<String> responsibleNodes = clusterManager.getResponsibleNodes(key);
        int writeQuorum = clusterManager.getWriteQuorum();
        AtomicInteger ackCount = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = responsibleNodes.stream()
                .map(node -> CompletableFuture.runAsync(() -> {
                    try {
                        if (node.equalsIgnoreCase(clusterManager.getCurrentNodeUrl())) {
                            cacheManager.put(key, value);
                            bloomFilterManager.add(key);
                            rocksDBManager.put(key, value);
                        } else {
                            restTemplate.postForObject(
                                    node + "/kv/replica/put?key=" + key + "&value=" + value,
                                    null, String.class
                            );
                        }
                        ackCount.incrementAndGet();
                    } catch (Exception e) {
                        hintedHandoffManager.storeHint(node,key,value);
                        System.err.println("Write failed for " + node + ": " + e.getMessage());
                    }
                }))
                .toList();

        try {
            // Wait up to 3 seconds for all to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.err.println("Write quorum timeout after 3 seconds");
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
        }

        return ackCount.get() >= writeQuorum;
    }


    public void replicaPut(String key, String value) {
        cacheManager.put(key, value);
        bloomFilterManager.add(key);
        rocksDBManager.put(key, value);
    }


    public String get(String key) {
        List<String> responsibleNodes = clusterManager.getResponsibleNodes(key);
        int successCount = 0;
        List<String> values = new ArrayList<>();

        for (String node : responsibleNodes) {
            try {
                String value;
                if (node.equalsIgnoreCase(clusterManager.getCurrentNodeUrl())) {
                    value = cacheManager.get(key);
                    if(value == null){
                        if(bloomFilterManager.mightContain(key)){
                            value = rocksDBManager.get(key);
                        }
                    }

                } else {
                    value = restTemplate.getForObject(node + "/kv/replica/get?key=" + key, String.class);
                }

                if (value != null) {
                    successCount++;
                    values.add(value);
                }

                if (successCount >= clusterManager.getReadQuorum()) {
                    // Return the most recent or majority value
                    return resolveConflict(values);
                }
            } catch (Exception e) {
                System.err.println("Read failed from " + node + ": " + e.getMessage());
            }
        }

        return null; // quorum not achieved
    }

    public String replicaGet(String key){
        String value = cacheManager.get(key);
        if(value != null) return value;
        if(bloomFilterManager.mightContain(key)) value = rocksDBManager.get(key);
        return value;
    }

    private String resolveConflict(List<String> values) {
        // For now, pick latest (last-write-wins)
        return values.get(values.size() - 1);
    }


    public void replicateLocally(String key, String value) {
        cacheManager.put(key, value);
        bloomFilterManager.add(key);
        rocksDBManager.put(key, value);
    }

    public void delete(String key) {
        List<String> responsibleNodes = clusterManager.getResponsibleNodes(key);
        for (String node : responsibleNodes) {
            if (node.equalsIgnoreCase(clusterManager.getCurrentNodeUrl())) {
                cacheManager.invalidate(key);
                rocksDBManager.delete(key);
            } else {
                restTemplate.delete(node + "/kv/replica/delete?key=" + key);
            }
        }
    }

    public void deleteReplica(String key) {
        cacheManager.invalidate(key);
        rocksDBManager.delete(key);
    }

    public Map<String, String> getAll() {
        Map<String, String> result = new HashMap<>();
        try (RocksIterator iterator = rocksDBManager.getDB().newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                String value = new String(iterator.value());
                result.put(key, value);
            }
        }
        return result;
    }
}

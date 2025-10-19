package com.distributekv.kvstore.service;

import com.distributekv.kvstore.db.RocksDBManager;
import com.distributekv.kvstore.filter.BloomFilterManager;
import jakarta.annotation.PostConstruct;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.stereotype.Service;
import com.distributekv.kvstore.cache.CacheManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KVStoreService {

    private final RocksDBManager rocksDBManager;
    private final CacheManager cacheManager;
    private final BloomFilterManager bloomFilterManager;


    public KVStoreService(RocksDBManager rocksDBManager, CacheManager cacheManager, BloomFilterManager bloomFilterManager) {
        this.rocksDBManager = rocksDBManager;
        this.cacheManager = cacheManager;
        this.bloomFilterManager = bloomFilterManager;
    }

    @PostConstruct
    public void rebuildBloomFilter() {
        List<String> allKeys = rocksDBManager.getAllKeys();
        for (String key : allKeys) {
            bloomFilterManager.add(key);
        }
    }

    public void put(String key, String value) {
        rocksDBManager.put(key, value);
        cacheManager.put(key, value);
        bloomFilterManager.add(key);
    }

    public String get(String key) {
        String cached = cacheManager.get(key);
        if (cached != null) {
            return cached + " (from cache)";
        }

        // 2️⃣ Check bloom filter before DB
        if (!bloomFilterManager.mightContain(key)) {
            return null; // definitely not there
        }

        // 3️⃣ Fetch from RocksDB
        String value = rocksDBManager.get(key);
        if (value != null) {
            cacheManager.put(key, value);
        }
        return value;
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


    public void delete(String key) {
        rocksDBManager.delete(key);
        cacheManager.invalidate(key);
    }
}

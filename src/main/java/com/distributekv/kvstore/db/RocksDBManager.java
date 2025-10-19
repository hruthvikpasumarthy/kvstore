package com.distributekv.kvstore.db;

import org.rocksdb.RocksIterator;
import org.springframework.stereotype.Component;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Component
public class RocksDBManager {

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;

    public RocksDBManager() {
        try {
            Path dbPath = Path.of("data/rocksdb");
            Files.createDirectories(dbPath);
            Options options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, dbPath.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
    }

    public RocksDB getDB() {
        return db;
    }

    /** Save or update a key-value pair */
    public void put(String key, String value) {
        try {
            db.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to put key: " + key, e);
        }
    }

    /** Retrieve value for a key */
    public String get(String key) {
        try {
            byte[] value = db.get(key.getBytes());
            return value != null ? new String(value) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get key: " + key, e);
        }
    }

    /** Delete a key-value pair */
    public void delete(String key) {
        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete key: " + key, e);
        }
    }

    public List<String> getAllKeys() {
        List<String> keys = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                keys.add(new String(iterator.key()));
            }
        }

        return keys;
    }



    public void close() {
        if (db != null) {
            db.close();
        }
    }
}
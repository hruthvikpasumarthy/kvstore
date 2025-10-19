package com.distributekv.kvstore.controller;

import com.distributekv.kvstore.service.KVStoreService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/kv")
public class KVStoreController {

    private final KVStoreService kvStoreService;

    public KVStoreController(KVStoreService kvStoreService) {
        this.kvStoreService = kvStoreService;
    }

    @PutMapping("/{key}")
    public ResponseEntity<String> put(@PathVariable String key, @RequestBody String value) {
        kvStoreService.put(key, value);
        return ResponseEntity.ok("Key stored successfully");
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        String value = kvStoreService.get(key);
        if (value == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(value);
    }

    @GetMapping
    public ResponseEntity<Map<String, String>> getAll() {
        return ResponseEntity.ok(kvStoreService.getAll());
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key) {
        kvStoreService.delete(key);
        return ResponseEntity.ok("Key deleted successfully");
    }
}

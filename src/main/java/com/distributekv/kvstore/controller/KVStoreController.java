package com.distributekv.kvstore.controller;

import com.distributekv.kvstore.service.KVStoreService;
import org.springframework.http.HttpStatus;
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

    @PostMapping("/put")
    public ResponseEntity<String> put(@RequestParam String key, @RequestParam String value) {
        boolean success = kvStoreService.put(key, value);
        return success ? ResponseEntity.ok("Write quorum achieved for key=" + key)
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Write quorum not met");
    }

    @PostMapping("/replica/put")
    public ResponseEntity<String> replicaPut(@RequestParam String key, @RequestParam String value) {
        try {
            kvStoreService.replicaPut(key,value);
            System.out.println("Replica write accepted for key: " + key);
            return ResponseEntity.ok("Replica stored");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Replica write failed");
        }
    }


    @GetMapping("/get")
    public ResponseEntity<String> get(@RequestParam String key) {
        String value = kvStoreService.get(key);
        if (value == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(value);
    }

    @GetMapping("/replica/get")
    public ResponseEntity<String> replicaGet(@RequestParam String key) {
        String value = kvStoreService.replicaGet(key);
        if (value == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(value);
    }

    @GetMapping
    public ResponseEntity<Map<String, String>> getAll() {
        return ResponseEntity.ok(kvStoreService.getAll());
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> delete(@RequestParam String key) {
        kvStoreService.delete(key);
        return ResponseEntity.ok("Key deleted successfully");
    }

    @DeleteMapping("/replica/delete")
    public ResponseEntity<String> replicaDelete(@RequestParam String key) {
        kvStoreService.deleteReplica(key);
        return ResponseEntity.ok("Key deleted successfully");
    }
}

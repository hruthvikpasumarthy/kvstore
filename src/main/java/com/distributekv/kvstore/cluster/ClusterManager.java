package com.distributekv.kvstore.cluster;

import com.distributekv.kvstore.hash.ConsistentHashRing;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import java.util.List;

@Component
@Getter
public class ClusterManager {

    @Value("${kvstore.nodeId}")
    private String currentNodeId;

    @Value("#{'${kvstore.nodes}'.split(',')}")
    private List<String> nodes;

    @Value("${kvstore.replicationFactor:3}")
    private int replicationFactor;

    @Value("${kvstore.quorum.write:2}")
    private int writeQuorum;

    @Value("${kvstore.quorum.read:2}")
    private int readQuorum;

    @Value("${server.port}")
    private int port;


    private ConsistentHashRing hashRing;

    @PostConstruct
    public void init() {
        this.hashRing = new ConsistentHashRing(nodes);
        System.out.println("Cluster initialized with nodes: " + nodes);
    }

    public List<String> getResponsibleNodes(String key) {
        return hashRing.getNodesForKey(key, replicationFactor);
    }

    public boolean isResponsibleNode(String key) {
        return getResponsibleNodes(key)
                .stream()
                .anyMatch(node -> node.contains(currentNodeId));
    }

    public String getCurrentNodeUrl() {
        // Build current node's URL dynamically
        return "http://localhost:" + port;
    }
}

package com.distributekv.kvstore.filter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class BloomFilterManager {


    private final BloomFilter<String> bloomFilter = BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8),
            10000, // expected insertions
            0.01   // 1% false positive rate
    );


    public void add(String key) {
        bloomFilter.put(key);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }
}

package com.distributekv.kvstore.hintedHandOff.scheduler;

import com.distributekv.kvstore.hintedHandOff.manager.HintedHandoffManager;
import com.distributekv.kvstore.hintedHandOff.model.Hint;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@RequiredArgsConstructor
public class HintDeliveryServiceScheduler {
    private final HintedHandoffManager hintManager;
    private final RestTemplate restTemplate = new RestTemplate();

    // retry every 15 seconds
    @Scheduled(fixedRate = 15000)
    public void deliverHints() {
        for (Map.Entry<String, CopyOnWriteArrayList<Hint>> entry : hintManager.getAllHints().entrySet()) {
            String node = entry.getKey();
            List<Hint> nodeHints = new ArrayList<>(entry.getValue());

            for (Hint h : nodeHints) {
                try {
                    restTemplate.postForObject(
                            node + "/kv/replica/put?key=" + h.getKey() + "&value=" + h.getValue(),
                            null, String.class);
                    System.out.printf("Delivered hint to %s (key=%s)%n", node, h.getKey());
                    hintManager.removeHint(node, h);
                } catch (Exception e) {
                    h.setRetryCount(h.getRetryCount() + 1);
                    System.out.printf("Node still unreachable: %s (retry=%d)%n",
                            node, h.getRetryCount());
                }
            }
        }
    }
}

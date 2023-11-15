package com.cecloud.rocketmq;

import com.cecloud.rocketmq.DTO.TransactionLog;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;

@Component
public class TransactionLogManager {
    private static final LinkedHashMap<String, TransactionLog> orderMap;

    static {
        orderMap = new LinkedHashMap<>(1000);
    }

    public void insert(TransactionLog log) {
        orderMap.put(log.getId(), log);
    }

    public void remove(String id) {
        orderMap.remove(id);
    }

    public boolean contains(String id) {
        return orderMap.containsKey(id);
    }

    public String list() {
        return orderMap.toString();
    }
}

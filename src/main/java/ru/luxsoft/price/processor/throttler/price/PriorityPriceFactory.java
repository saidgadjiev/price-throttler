package ru.luxsoft.price.processor.throttler.price;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class controls price creation process. We need this to work correctly with price priorities.
 */
public class PriorityPriceFactory {

    private final Map<String, Integer> priorityMap = new ConcurrentHashMap<>();

    public PriorityPrice createNewPriceOrChangePriority(String ccyPair, double rate) {
        PriorityPrice priorityPrice = new PriorityPrice(ccyPair, rate);
        if (priorityMap.containsKey(ccyPair)) {
            priorityPrice.setPriority(priorityMap.get(ccyPair) + 1);
        }
        priorityMap.put(ccyPair, priorityPrice.getPriority());

        return priorityPrice;
    }
}

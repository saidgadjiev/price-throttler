package ru.luxsoft.price.processor.subscriber;

import ru.luxsoft.price.processor.PriceProcessor;

import java.util.concurrent.atomic.AtomicInteger;

public class FastSubscriber implements PriceProcessor {

    private AtomicInteger counter;

    public FastSubscriber(AtomicInteger counter) {
        this.counter = counter;
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        new FastTask().run();
        counter.incrementAndGet();
    }
}

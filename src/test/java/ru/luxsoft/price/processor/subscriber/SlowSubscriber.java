package ru.luxsoft.price.processor.subscriber;

import ru.luxsoft.price.processor.PriceProcessor;

import java.util.concurrent.atomic.AtomicInteger;

public class SlowSubscriber implements PriceProcessor {

    private AtomicInteger counter;

    public SlowSubscriber(AtomicInteger counter) {
        this.counter = counter;
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        new SlowTask().run();
        counter.incrementAndGet();
    }
}

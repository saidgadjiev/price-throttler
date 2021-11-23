package ru.luxsoft.price.processor.subscriber;

import ru.luxsoft.price.processor.PriceProcessor;

import java.util.concurrent.atomic.AtomicInteger;

public class ScreenSubscriber implements PriceProcessor {

    private AtomicInteger handledPricesCounter;

    public ScreenSubscriber(AtomicInteger handledPricesCounter) {
        this.handledPricesCounter = handledPricesCounter;
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        showPriceOnScreen();
        handledPricesCounter.incrementAndGet();
    }

    private void showPriceOnScreen() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

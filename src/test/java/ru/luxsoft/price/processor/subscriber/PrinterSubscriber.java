package ru.luxsoft.price.processor.subscriber;

import ru.luxsoft.price.processor.PriceProcessor;

import java.util.concurrent.atomic.AtomicInteger;

public class PrinterSubscriber implements PriceProcessor {

    private AtomicInteger handledPricesCounter;

    public PrinterSubscriber(AtomicInteger handledPricesCounter) {
        this.handledPricesCounter = handledPricesCounter;
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        printPriceOnPaper();
        handledPricesCounter.incrementAndGet();
    }

    private void printPriceOnPaper() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

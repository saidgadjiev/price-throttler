package ru.luxsoft.price.processor.subscriber;

public class SlowTask {

    public void run() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

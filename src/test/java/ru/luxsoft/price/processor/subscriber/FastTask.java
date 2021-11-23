package ru.luxsoft.price.processor.subscriber;

public class FastTask {

    public void run() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

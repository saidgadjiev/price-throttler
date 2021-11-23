package ru.luxsoft.price.processor;

import ru.luxsoft.price.processor.subscriber.FastSubscriber;
import ru.luxsoft.price.processor.subscriber.SlowSubscriber;
import ru.luxsoft.price.processor.throttler.PriceThrottler;

import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

class PriceThrottlerTest {

    private static final AtomicInteger SLOW_COUNTER = new AtomicInteger();

    private static final AtomicInteger FAST_COUNTER = new AtomicInteger();

    public static void main(String[] args) {
        PriceThrottler priceThrottler = new PriceThrottler();
        for (int i = 0; i < 10; ++i) {
            priceThrottler.subscribe(new SlowSubscriber(SLOW_COUNTER));
        }
        for (int i = 0; i < 20; ++i) {
            priceThrottler.subscribe(new FastSubscriber(FAST_COUNTER));
        }

        Random random = new Random();
        for (int i = 0; i < 30; ++i) {
            priceThrottler.onPrice("EURRUB" + random.nextInt(5), random.nextInt(70));
        }

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String cmd = scanner.next();
            switch (cmd) {
                case "s":
                    System.out.println("Slow counter " + SLOW_COUNTER.get());
                    System.out.println("Fast counter " + FAST_COUNTER.get());
                    priceThrottler.printStats();
                    break;
            }
        }
    }
}
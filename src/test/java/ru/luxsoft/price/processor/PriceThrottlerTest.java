package ru.luxsoft.price.processor;

import ru.luxsoft.price.processor.subscriber.PrinterSubscriber;
import ru.luxsoft.price.processor.subscriber.ScreenSubscriber;
import ru.luxsoft.price.processor.throttler.PriceThrottler;

import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

//Просто класс с main методом для тестов
class PriceThrottlerTest {

    private static final int SLOW_SUBSCRIBERS_PARALLELISM = Runtime.getRuntime().availableProcessors() / 2;

    private static final int FAST_SUBSCRIBERS_PARALLELISM = Runtime.getRuntime().availableProcessors() / 2;

    private static final int SLOW_SUBSCRIBER_RUN_TIME_LOWER_BOUND_IN_SECONDS = 1;

    private static final AtomicInteger SLOW_HANDLED_PRICES_COUNTER = new AtomicInteger();

    private static final AtomicInteger FAST_HANDLED_PRICES_COUNTER = new AtomicInteger();

    private static final int TEST_SLOW_SUBSCRIBERS_COUNT = 10;

    private static final int TEST_FAST_SUBSCRIBERS_COUNT = 20;

    private static final int TEST_PRICES_COUNT = 10;

    public static void main(String[] args) {
        PriceThrottler priceThrottler = createThrottler();
        publishTestPrices(priceThrottler);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String cmd = scanner.next();
            if (cmd.equals("s")) {
                System.out.println("Throttler statistic:");
                System.out.println("Slow handled prices counter " + SLOW_HANDLED_PRICES_COUNTER.get());
                System.out.println("Fast handled prices counter " + FAST_HANDLED_PRICES_COUNTER.get());
                priceThrottler.printStats();
            } else if (cmd.equals("q")) {
                System.out.println("Shutting down...");
                priceThrottler.shutdown();
                break;
            }
        }
    }

    private static void publishTestPrices(PriceThrottler priceThrottler) {
        Random random = new Random();
        Set<String> ccPairs = new HashSet<>();
        for (int i = 0; i < TEST_PRICES_COUNT; ++i) {
            String ccPair = "EURRUB" + random.nextInt(5);
            ccPairs.add(ccPair);
            priceThrottler.onPrice(ccPair, random.nextInt(70));
        }

        System.out.println("ccPairs " + ccPairs.toString());
    }

    private static PriceThrottler createThrottler() {
        PriceThrottler priceThrottler = new PriceThrottler(SLOW_SUBSCRIBERS_PARALLELISM, FAST_SUBSCRIBERS_PARALLELISM,
                SLOW_SUBSCRIBER_RUN_TIME_LOWER_BOUND_IN_SECONDS);
        for (int i = 0; i < TEST_SLOW_SUBSCRIBERS_COUNT; ++i) {
            priceThrottler.subscribe(new PrinterSubscriber(SLOW_HANDLED_PRICES_COUNTER));
        }
        for (int i = 0; i < TEST_FAST_SUBSCRIBERS_COUNT; ++i) {
            priceThrottler.subscribe(new ScreenSubscriber(FAST_HANDLED_PRICES_COUNTER));
        }

        return priceThrottler;
    }
}
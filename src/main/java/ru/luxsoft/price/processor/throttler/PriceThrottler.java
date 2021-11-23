package ru.luxsoft.price.processor.throttler;

import org.apache.log4j.Logger;
import ru.luxsoft.price.processor.PriceProcessor;
import ru.luxsoft.price.processor.throttler.price.PriorityPrice;
import ru.luxsoft.price.processor.throttler.price.PriorityPriceFactory;
import ru.luxsoft.price.processor.throttler.rapidity.TaskRapidityResolver;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Main throttler class. To increase throughput increase the slowSubscribersParallelism and fastSubscribersParallelism
 * constructor parameters.
 * To control which subscribers slow and which fast use the slowSubscriberRunTimeLowerBoundInSeconds constructor parameter and
 * throttler will detect subscriber's rapidity.
 */
public class PriceThrottler implements PriceProcessor {

    private static final Logger LOGGER = Logger.getLogger(PriceThrottler.class);

    private final PriceProcessorsContainer priceProcessors = new PriceProcessorsContainer();

    private final PricePublisher pricePublisher;

    private final PriorityPriceFactory priceCreator = new PriorityPriceFactory();

    public PriceThrottler(int slowSubscribersParallelism, int fastSubscribersParallelism,
                          int slowSubscriberRunTimeLowerBoundInSeconds) {
        this.pricePublisher = new PricePublisher(
                (ThreadPoolExecutor) Executors.newFixedThreadPool(slowSubscribersParallelism),
                (ThreadPoolExecutor) Executors.newFixedThreadPool(fastSubscribersParallelism),
                new TaskRapidityResolver(slowSubscriberRunTimeLowerBoundInSeconds),
                priceProcessors
        );
        pricePublisher.start();

        LOGGER.info("Price throttler started");
        LOGGER.info("Slow subscribers parallelism " + slowSubscribersParallelism);
        LOGGER.info("Fast subscribers parallelism " + fastSubscribersParallelism);
        LOGGER.info("Slow subscriber run time lower bound in seconds " + slowSubscriberRunTimeLowerBoundInSeconds);
    }

    public void onPrice(String ccyPair, double rate) {
        PriorityPrice priorityPrice = priceCreator.createNewPriceOrChangePriority(ccyPair, rate);
        priceProcessors.addOrReplacePrice(priorityPrice);
    }

    public void subscribe(PriceProcessor priceProcessor) {
        priceProcessors.addProcessor(priceProcessor);
    }

    public void unsubscribe(PriceProcessor priceProcessor) {
        priceProcessors.removeProcessor(priceProcessor);
    }

    public void printStats() {
        priceProcessors.printStats();
        pricePublisher.printStats();
    }

    public void shutdown() {
        pricePublisher.shutdown();
    }
}

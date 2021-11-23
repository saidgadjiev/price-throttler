package ru.luxsoft.price.processor.throttler;

import ru.luxsoft.price.processor.PriceProcessor;
import ru.luxsoft.price.processor.throttler.price.PriorityPrice;
import ru.luxsoft.price.processor.throttler.rapidity.TaskRapidityResolver;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class represents scheduled prices publisher to subscribers. We need this because we wan't affect
 * slow subscribers to fast subscribers. So the class will publish prices to slow and fast subscribers
 * concurrently in different thread pools.
 */
class PricePublisher {

    private static final int SCHEDULE_TIME_IN_MILLIS = 1000;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final List<PriceProcessor> inProgressProcessors = Collections.synchronizedList(new ArrayList<>());

    private final ThreadPoolExecutor fastSubscribersThreadPool;

    private final ThreadPoolExecutor slowSubscribersThreadPool;

    private final TaskRapidityResolver taskRapidityResolver;

    private PriceProcessorsContainer priceProcessors;

    PricePublisher(ThreadPoolExecutor slowSubscribersThreadPool, ThreadPoolExecutor fastSubscribersThreadPool,
                   TaskRapidityResolver taskRapidityResolver, PriceProcessorsContainer priceProcessors) {
        this.priceProcessors = priceProcessors;
        this.fastSubscribersThreadPool = fastSubscribersThreadPool;
        this.slowSubscribersThreadPool = slowSubscribersThreadPool;
        this.taskRapidityResolver = taskRapidityResolver;
    }

    void shutdown() {
        shutdownExecutorService(scheduledExecutorService);
        shutdownExecutorService(slowSubscribersThreadPool);
        shutdownExecutorService(fastSubscribersThreadPool);
    }

    void start() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            publishToFastSubscribers();
            publishToSlowSubscribers();
        }, SCHEDULE_TIME_IN_MILLIS, SCHEDULE_TIME_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    void printStats() {
        System.out.println("In progress subscribers count " + inProgressProcessors.size());

        Set<PriceProcessor> priceProcessorsSet = this.priceProcessors.getPriceProcessors();
        for (PriceProcessor priceProcessor : priceProcessorsSet) {
            BlockingQueue<PriorityPrice> processorQueue = priceProcessors.getProcessorQueue(priceProcessor);
            TaskRapidityResolver.Rapidity rapidity = priceProcessors.getRapidity(priceProcessor);
            System.out.println(rapidity.name() + " processor queue size " + processorQueue.size());
        }
    }

    private void publishToFastSubscribers() {
        int freeThreads = fastSubscribersThreadPool.getCorePoolSize() - fastSubscribersThreadPool.getActiveCount();
        if (freeThreads > 0) {
            for (int i = 0; i < freeThreads; ++i) {
                PriceProcessor fastProcessor = priceProcessors.getNextFastProcessor();
                if (fastProcessor == null) {
                    return;
                }
                if (!inProgressProcessors.contains(fastProcessor)) {
                    PriorityPrice price = priceProcessors.getNextPrice(fastProcessor);
                    if (price == null) {
                        return;
                    }
                    inProgressProcessors.add(fastProcessor);
                    fastSubscribersThreadPool.execute(() -> {
                        fastProcessor.onPrice(price.getCcyPair(), price.getRate());
                        inProgressProcessors.remove(fastProcessor);
                    });
                }
            }
        }
    }

    private void publishToSlowSubscribers() {
        int freeThreads = fastSubscribersThreadPool.getCorePoolSize() - fastSubscribersThreadPool.getActiveCount();
        if (freeThreads > 0) {
            for (int i = 0; i < freeThreads; ++i) {
                PriceProcessor slowProcessor = priceProcessors.getNextProcessor();
                if (slowProcessor == null) {
                    return;
                }
                if (!inProgressProcessors.contains(slowProcessor)) {
                    PriorityPrice price = priceProcessors.getNextPrice(slowProcessor);
                    if (price == null) {
                        return;
                    }
                    inProgressProcessors.add(slowProcessor);
                    slowSubscribersThreadPool.execute(() -> {
                        TaskRapidityResolver.Rapidity rapidity = taskRapidityResolver.executeTask(() -> {
                            slowProcessor.onPrice(price.getCcyPair(), price.getRate());
                        });
                        if (rapidity.equals(TaskRapidityResolver.Rapidity.FAST)) {
                            priceProcessors.markProcessorAsFast(slowProcessor);
                        }
                        inProgressProcessors.remove(slowProcessor);
                    });
                }
            }
        }
    }

    private void shutdownExecutorService(ExecutorService executorService) {
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

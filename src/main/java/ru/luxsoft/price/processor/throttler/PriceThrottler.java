package ru.luxsoft.price.processor.throttler;

import org.apache.log4j.Logger;
import ru.luxsoft.price.processor.PriceProcessor;
import ru.luxsoft.price.processor.throttler.rapidity.TaskRapidityResolver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class PriceThrottler implements PriceProcessor {

    private static final Logger LOGGER = Logger.getLogger(PriceThrottler.class);

    private final ThreadPoolExecutor fastSubscribersThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

    private final ThreadPoolExecutor slowSubscribersThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final Map<String, Integer> priorityMap = new ConcurrentHashMap<>();

    private final List<PriceProcessor> slowProcessors = new CopyOnWriteArrayList<>();

    private final List<PriceProcessor> inProgressProcessors = new CopyOnWriteArrayList<>();

    private final TaskRapidityResolver taskRapidityResolver = new TaskRapidityResolver(1);

    private final Map<PriceProcessor, PriorityBlockingQueue<PriorityPrice>> priceProcessorBlockingQueueMap = new ConcurrentHashMap<>();

    public PriceThrottler() {
        scheduleThrottler();

        LOGGER.info("Price throttler started");
    }

    public void onPrice(String ccyPair, double rate) {
        PriorityPrice priorityPrice = new PriorityPrice(ccyPair, rate);
        if (priorityMap.containsKey(ccyPair)) {
            priorityPrice.setPriority(priorityMap.get(ccyPair) + 1);
        }
        priorityMap.put(ccyPair, priorityPrice.priority);

        priceProcessorBlockingQueueMap.forEach((priceProcessor, priorityPrices) -> pushPrice(priorityPrices, priorityPrice));
    }

    public void subscribe(PriceProcessor priceProcessor) {
        priceProcessorBlockingQueueMap.put(priceProcessor, new PriorityBlockingQueue<>());
        slowProcessors.add(priceProcessor);
    }

    public void unsubscribe(PriceProcessor priceProcessor) {
        priceProcessorBlockingQueueMap.remove(priceProcessor);
        slowProcessors.remove(priceProcessor);
    }

    private void scheduleThrottler() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            publishToFastSubscribers();
            publishToSlowSubscribers();
        }, 50, 1000, TimeUnit.MILLISECONDS);
    }

    public void printStats() {
        System.out.println("Slow subscribers count " + slowProcessors.size());
        System.out.println("Fast subscribers count " + (priceProcessorBlockingQueueMap.keySet().size() - slowProcessors.size()));
        System.out.println("All processors count " + priceProcessorBlockingQueueMap.keySet().size());
        System.out.println("In progress subscribers count " + inProgressProcessors.size());

        priceProcessorBlockingQueueMap.forEach((priceProcessor, priorityPrices) -> {
            System.out.print((slowProcessors.contains(priceProcessor) ? "Slow " : "Fast ") + " processor queue size " + priorityPrices.size());
            PriorityPrice priorityPrice = priorityPrices.peek();
            if (priorityPrice != null) {
                System.out.print(" : ");
                System.out.print(priorityPrice.ccyPair + "=" + priorityPrice.rate + "->" + priorityPrice.priority);
            }
            System.out.println();
        });
    }

    private void publishToFastSubscribers() {
        boolean hasFreeThreads = fastSubscribersThreadPool.getCorePoolSize() - fastSubscribersThreadPool.getActiveCount() > 0;
        if (hasFreeThreads) {
            priceProcessorBlockingQueueMap.forEach((priceProcessor, priorityPrices) -> {
                if (!slowProcessors.contains(priceProcessor) && !inProgressProcessors.contains(priceProcessor)) {
                    PriorityPrice price = priorityPrices.poll();
                    if (price == null) {
                        return;
                    }
                    inProgressProcessors.add(priceProcessor);
                    fastSubscribersThreadPool.execute(() -> {
                        priceProcessor.onPrice(price.ccyPair, price.rate);
                        inProgressProcessors.remove(priceProcessor);
                    });
                }
            });
        }
    }

    private void publishToSlowSubscribers() {
        boolean hasFreeThreads = slowSubscribersThreadPool.getCorePoolSize() - slowSubscribersThreadPool.getActiveCount() > 0;
        if (hasFreeThreads) {
            priceProcessorBlockingQueueMap.forEach((priceProcessor, priorityPrices) -> {
                if (slowProcessors.contains(priceProcessor) && !inProgressProcessors.contains(priceProcessor)) {
                    PriorityPrice price = priorityPrices.poll();
                    if (price == null) {
                        return;
                    }
                    inProgressProcessors.add(priceProcessor);
                    slowSubscribersThreadPool.execute(() -> {
                        TaskRapidityResolver.Rapidity rapidity = taskRapidityResolver.executeTask(() -> priceProcessor.onPrice(price.ccyPair, price.rate));
                        if (rapidity.equals(TaskRapidityResolver.Rapidity.FAST)) {
                            slowProcessors.remove(priceProcessor);
                        }
                        inProgressProcessors.remove(priceProcessor);
                    });
                }
            });
        }
    }

    private void pushPrice(PriorityBlockingQueue<PriorityPrice> queue, PriorityPrice priorityPrice) {
        queue.remove(priorityPrice);
        queue.add(priorityPrice);
    }

    private static class PriorityPrice implements Comparable<PriorityPrice> {

        private int priority = 0;

        private String ccyPair;

        private double rate;

        PriorityPrice(String ccyPair, double rate) {
            this.ccyPair = ccyPair;
            this.rate = rate;
        }

        void setPriority(int priority) {
            this.priority = priority;
        }

        @Override
        public int compareTo(PriorityPrice o) {
            return priority < o.priority ? 1 : priority == o.priority ? 0 : -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PriorityPrice that = (PriorityPrice) o;

            return ccyPair.equals(that.ccyPair);
        }

        @Override
        public int hashCode() {
            return ccyPair.hashCode();
        }
    }
}

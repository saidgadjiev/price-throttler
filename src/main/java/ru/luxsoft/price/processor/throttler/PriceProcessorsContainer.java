package ru.luxsoft.price.processor.throttler;

import ru.luxsoft.price.processor.PriceProcessor;
import ru.luxsoft.price.processor.throttler.price.PriorityPrice;
import ru.luxsoft.price.processor.throttler.rapidity.TaskRapidityResolver;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * This class holds processors and controls processors invocation queue. We need this because
 * we can't publish price to all subscribers at the same time. So we need organize the queue.
 */
class PriceProcessorsContainer {

    private final Map<PriceProcessor, PriorityBlockingQueue<PriorityPrice>> priceProcessorBlockingQueueMap = new ConcurrentHashMap<>();

    private final Map<PriceProcessor, TaskRapidityResolver.Rapidity> processorRapidityMap = new ConcurrentHashMap<>();

    private final BlockingQueue<PriceProcessor> slowProcessorsQueue = new LinkedBlockingQueue<>();

    private final BlockingQueue<PriceProcessor> fastProcessorsQueue = new LinkedBlockingQueue<>();

    void addProcessor(PriceProcessor priceProcessor) {
        priceProcessorBlockingQueueMap.put(priceProcessor, new PriorityBlockingQueue<>());
        processorRapidityMap.put(priceProcessor, TaskRapidityResolver.Rapidity.SLOW);

        slowProcessorsQueue.add(priceProcessor);
    }

    synchronized void removeProcessor(PriceProcessor priceProcessor) {
        slowProcessorsQueue.remove(priceProcessor);
        fastProcessorsQueue.remove(priceProcessor);

        priceProcessorBlockingQueueMap.remove(priceProcessor);
        processorRapidityMap.remove(priceProcessor);
    }

    public long getProcessorsCount(TaskRapidityResolver.Rapidity rapidity) {
        return processorRapidityMap.values().stream().filter(f -> f.equals(rapidity)).count();
    }

    long getProcessorsCount() {
        return processorRapidityMap.size();
    }

    Set<PriceProcessor> getPriceProcessors() {
        return processorRapidityMap.keySet();
    }

    BlockingQueue<PriorityPrice> getProcessorQueue(PriceProcessor priceProcessor) {
        return priceProcessorBlockingQueueMap.get(priceProcessor);
    }

    TaskRapidityResolver.Rapidity getRapidity(PriceProcessor priceProcessor) {
        return processorRapidityMap.get(priceProcessor);
    }

    void addOrReplacePrice(PriorityPrice priorityPrice) {
        priceProcessorBlockingQueueMap.forEach((priceProcessor, priorityPrices) -> {
            priorityPrices.remove(priorityPrice);
            priorityPrices.add(priorityPrice);
        });
    }

    synchronized PriceProcessor getNextProcessor() {
        PriceProcessor processor = slowProcessorsQueue.poll();
        if (processor == null) {
            return null;
        }
        slowProcessorsQueue.add(processor);

        return processor;
    }

    synchronized PriceProcessor getNextFastProcessor() {
        PriceProcessor processor = fastProcessorsQueue.poll();
        if (processor == null) {
            return null;
        }
        fastProcessorsQueue.add(processor);

        return processor;
    }

    PriorityPrice getNextPrice(PriceProcessor priceProcessor) {
        return priceProcessorBlockingQueueMap.get(priceProcessor).poll();
    }

    synchronized void markProcessorAsFast(PriceProcessor priceProcessor) {
        slowProcessorsQueue.remove(priceProcessor);
        processorRapidityMap.put(priceProcessor, TaskRapidityResolver.Rapidity.FAST);
        fastProcessorsQueue.add(priceProcessor);
    }

    void printStats() {
        System.out.println("Slow subscribers count " + getProcessorsCount(TaskRapidityResolver.Rapidity.SLOW));
        System.out.println("Fast subscribers count " + getProcessorsCount(TaskRapidityResolver.Rapidity.FAST));
        System.out.println("Slow subscribers queue size " + slowProcessorsQueue.size());
        System.out.println("Fast subscribers queue size " + fastProcessorsQueue.size());
        System.out.println("All processors count " + getProcessorsCount());
    }
}

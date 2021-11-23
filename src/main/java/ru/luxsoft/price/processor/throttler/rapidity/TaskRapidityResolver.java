package ru.luxsoft.price.processor.throttler.rapidity;

import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.TimeUnit;

public class TaskRapidityResolver {

    private long slowTaskLowerBoundInSeconds;

    public TaskRapidityResolver(long slowTaskLowerBoundInSeconds) {
        this.slowTaskLowerBoundInSeconds = slowTaskLowerBoundInSeconds;
    }

    public Rapidity executeTask(Runnable runnable) {
        StopWatch timeMeter = new StopWatch();
        timeMeter.start();
        runnable.run();
        timeMeter.stop();

        long seconds = timeMeter.getTime(TimeUnit.SECONDS);

        return seconds >= slowTaskLowerBoundInSeconds ? Rapidity.SLOW : Rapidity.FAST;
    }

    public enum Rapidity {

        SLOW,

        FAST
    }
}

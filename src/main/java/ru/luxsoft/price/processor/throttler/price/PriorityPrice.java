package ru.luxsoft.price.processor.throttler.price;

/**
 * This class represents price objects. We need priority because less frequently ccPairs should be published at first
 * rather than most frequently ccPairs. The lower the priority, the earlier the price will be processed.
 */
public class PriorityPrice implements Comparable<PriorityPrice> {

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

    public double getRate() {
        return rate;
    }

    public int getPriority() {
        return priority;
    }

    public String getCcyPair() {
        return ccyPair;
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

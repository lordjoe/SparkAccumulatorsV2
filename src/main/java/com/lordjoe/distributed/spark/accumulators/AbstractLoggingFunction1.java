package com.lordjoe.distributed.spark.accumulators;

import com.lordjoe.algorithms.Long_Formatter;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction1
 * stand in for  Function1
 * superclass for defined functions that will log on first call making it easier to see
 * do work in doCall
 * User: Steve
 * Date: 10/23/2014
 */
public  abstract  class AbstractLoggingFunction1<T1 extends Serializable,  R extends Serializable>
        extends AbstractFunction1<T1, R> {

    // write ever y time the function is called this many times
    private static int callReportInterval = 50000;

    public static int getCallReportInterval() {
        return callReportInterval;
    }

    @SuppressWarnings("unused")
    public static void setCallReportInterval(final int pCallReportInterval) {
        callReportInterval = pCallReportInterval;
    }

    private static transient boolean logged;   // transient so every machine keeps its own
    private transient long numberCalls;   // transient so every machine keeps its own
    private transient long startTime;     // transient so every machine keeps its own
    private transient long totalTime;
    private transient long accumulatedTime;
    private final ISparkAccumulators accumulators;


    protected AbstractLoggingFunction1() {
        // make an accumulator if one does not exist - should happen in hte executor
        accumulators = AccumulatorUtilities.getInstance();
        if (!isFunctionCallsLogged())
            return;
        // build an accumulator for this function
        if (accumulators != null) {
            String className = getClass().getSimpleName();
            accumulators.createFunctionAccumulator(className);
        }
    }

    public long getTotalTime() {
        return totalTime;
    }


    public long getRunningTimeMillisec() {
        return System.currentTimeMillis() - getStartTIme();
    }

    /**
     * really the time this function was first called as a local copy
     *
     * @return  time
     */
    public long getStartTIme() {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }
        return startTime;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getAccumulatedTime() {
        return accumulatedTime;
    }

    public void incrementAccumulatedTime(long added) {
        accumulatedTime += added;
        totalTime += added;
    }


    /**
     * as it says
     * @return  time
     */
    public long getAndClearAccumulatedTime() {
        long ret = accumulatedTime;
        accumulatedTime = 0;
        return ret;
    }

    /**
     * Override this to prevent logging
     *
     * @return  true if we track timing
     */
    public boolean isFunctionCallsLogged() {
        return AccumulatorUtilities.isFunctionsLoggedByDefault();
    }

    public final boolean isLogged() {
        return logged;
    }

    public final void setLogged(final boolean pLogged) {
        logged = pLogged;
    }

    public final long getNumberCalls() {
        return numberCalls;
    }

    public final void incrementNumberCalled() {
        numberCalls++;
    }

    public ISparkAccumulators getAccumulators() {
        return accumulators;
    }

    public static final double MILLISEC_IN_NANOSEC = 1000 * 1000;
    public static final double SEC_IN_NANOSEC = MILLISEC_IN_NANOSEC * 1000;
    public static final double MIN_IN_NANOSEC = SEC_IN_NANOSEC * 60;
    public static final double HOUR_IN_NANOSEC = MIN_IN_NANOSEC * 60;
    public static final double DAY_IN_NANOSEC = HOUR_IN_NANOSEC * 24;

    public static String formatNanosec(long timeNanosec) {
        if (Math.abs(timeNanosec) < 10 * SEC_IN_NANOSEC)
            return String.format("%10.2f", timeNanosec / MILLISEC_IN_NANOSEC) + " msec";
        if (Math.abs(timeNanosec) < 10 * MIN_IN_NANOSEC)
            return String.format("%10.2f", timeNanosec / SEC_IN_NANOSEC) + " sec";
        if (Math.abs(timeNanosec) < 10 * HOUR_IN_NANOSEC)
            return String.format("%10.2f", timeNanosec / MIN_IN_NANOSEC) + " min";
        if (Math.abs(timeNanosec) < 10 * DAY_IN_NANOSEC)
            return String.format("%10.2f", timeNanosec / HOUR_IN_NANOSEC) + " hour";
        return String.format("%10.2f", timeNanosec / DAY_IN_NANOSEC) + " days";
    }

    public static String formatMillisec(long timeMillisec) {
        return formatNanosec(timeMillisec * (long)MILLISEC_IN_NANOSEC);
    }

    public void reportCalls() {
        if (!isFunctionCallsLogged())
            return;
        String className = getClass().getSimpleName();
        if (!isLogged()) {
            System.err.println("Starting Function " + className);
            setLogged(true);  // done once
        }
        // report every 100,000 calls
        if (getCallReportInterval() > 0) {
            long numberCalls1 = getNumberCalls();
            if (numberCalls1 > 0 && numberCalls1 % getCallReportInterval() == 0) {
                System.err.println("Calling Function " + className + " " + Long_Formatter.format(numberCalls1) + " times");
                System.err.println(" Function took " + className + " " + formatNanosec(totalTime) + " running for " + formatMillisec(getRunningTimeMillisec()));
            }
        }
        incrementNumberCalled();

        ISparkAccumulators accumulators1 = getAccumulators();
        if (accumulators1 == null)
            return;
        long time = getAndClearAccumulatedTime();
        accumulators1.incrementFunctionAccumulator(className, time);

    }


    /**
     * do work here
     *
     * @param v1 inpu
     * @return
     */
    public abstract R doApply(final T1 v1);


    @Override
    public R apply(T1 v1) {
        reportCalls();
        long startTime = System.nanoTime();
        R ret = doApply(v1 );
        long estimatedTime = System.nanoTime() - startTime;
        incrementAccumulatedTime(estimatedTime);
        return ret;
    }


}

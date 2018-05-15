package com.lordjoe.distributed.spark.accumulators;

import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * com.lordjoe.distributed.spark.accumulators.LogRareEventsAccumulator
 * Accululator to look at garbage collection use
 * call checkGCTime()  to save gc use
 *
 * @author Steve Lewis
 * @date 5/28/2015
 */
public class LogRareEventsAccumulator extends IAccumulator<LogRareEventsAccumulator> {

    public static final String LOG_RARE_EVENTS_NAME = "LogRareEventsAccumulator";


    public static class RareEvent {
        private final long time = System.currentTimeMillis();
        public final String macAddress = MachineUseAccumulator.getMacAddress();
        public final String message;

        public RareEvent(final String pBean) {
            message = pBean;
        }

        @Override
        public String toString() {

            Date date = new Date(time);
            DateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
            String dateFormatted = formatter.format(date);
            return "RareEvent{" +
                    "time=" + dateFormatted +
                    ", macAddress='" + macAddress + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }


    // how to build this type of accumulator

    public static LogRareEventsAccumulator empty() {
        return new LogRareEventsAccumulator();
    }

    private final Set<RareEvent> myLog = new HashSet<RareEvent>();

    /**
     * Use static method empty
     */
    private LogRareEventsAccumulator() {

    }

    /**
     * public methis to create to make log message
     * @param message
     */
    public LogRareEventsAccumulator(String message) {
          this();
         myLog.add(new RareEvent(message));
       }


    protected void checkGCTime() {
        throw new UnsupportedOperationException("Fix This"); // ToDo
     }

    @Override
    public boolean isZero() {
        return myLog.isEmpty();
    }

    @Override
    public IAccumulator<LogRareEventsAccumulator> copy() {
        LogRareEventsAccumulator ret = new LogRareEventsAccumulator();
        ret.myLog.addAll(myLog);
        return ret;
    }

    @Override
    public void merge(AccumulatorV2<LogRareEventsAccumulator, LogRareEventsAccumulator> other) {
        myLog.addAll(((LogRareEventsAccumulator)other).myLog);
    }

    @Override
    public LogRareEventsAccumulator value() {
        LogRareEventsAccumulator ret = new LogRareEventsAccumulator();
        ret.myLog.addAll(myLog);
        return ret;
    }

    @Override
    public void reset() {
        myLog.clear();
    }


    /**
     * given a value return it as 0
     * default behavior os th return the value itself
     *
     * @return
     */
    @Override
    public LogRareEventsAccumulator asZero() {
        return empty();
    }


    public void add(LogRareEventsAccumulator added) {
        myLog.addAll(added.myLog);
       }


    /**
     * like toString but might add more information than a shorter string
     * usually implemented bu appending toString
     *
     * @param out
     */
    @Override
    public void buildReport(final Appendable out) {
        try {
            out.append(toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
         if(myLog.isEmpty())
             sb.append(" None") ;
        else
             sb.append("\n");
          //    throw new UnsupportedOperationException("Fix This"); // ToDo
        for (RareEvent rareEvent : myLog) {
            sb.append(rareEvent.toString());
            sb.append("\n");

        }
        return sb.toString();
    }


}

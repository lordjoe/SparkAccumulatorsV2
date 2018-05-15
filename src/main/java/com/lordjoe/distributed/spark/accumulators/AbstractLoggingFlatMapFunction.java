package com.lordjoe.distributed.spark.accumulators;

import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.Iterator;

/**
 * org.apache.spark.api.java.function.AbstraceLoggingFunction
 * stand in for  FlatMapFunction
 * superclass for defined functions that will log on first call making it easier to see
 * do work in doCall
 * User: Steve
 * Date: 10/23/2014
 */
public abstract class AbstractLoggingFlatMapFunction<T, R extends Serializable>
        extends AbstractLoggingFunctionBase implements FlatMapFunction<T, R> {


    /**
     * NOTE override doCall not this
     *
     * @param t
     * @return
     */
    @Override
    public final Iterator<R> call(final T t) throws Exception {
        reportCalls();
        long startTime = System.nanoTime();
        Iterator<R> ret =  doCall(t);
        long estimatedTime = System.nanoTime() - startTime;
        incrementAccumulatedTime(estimatedTime);
         return ret;
    }

    /**
     * do work here
     *
     * @param v1
     * @return
     */
     public abstract Iterator<R> doCall(final T t) throws Exception;

}

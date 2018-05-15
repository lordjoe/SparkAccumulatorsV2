package com.lordjoe.distributed.spark.accumulators;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import java.io.Serializable;

/**
 * org.apache.spark.api.java.function.AbstraceLoggingFunction
 * stand in for  ReduceFunction
 * superclass for defined functions that will log on first call making it easier to see
 * do work in doCall
 * User: Steve
 * Date: 10/23/2014
 */
public  abstract class AbstractLoggingReduceFunction<T extends Serializable>
        extends AbstractLoggingFunctionBase implements ReduceFunction<T> {

     /**
     * do work here
     *
     * @param v1
     * @return
     */
     public abstract T doCall(T t, T t1) throws Exception;

    @Override
    public T call(T t, T t1) throws Exception {
        reportCalls();
        long startTime = System.nanoTime();
        T ret =  doCall(t,t1);
        long estimatedTime = System.nanoTime() - startTime;
        incrementAccumulatedTime(estimatedTime);
        return ret;
    }
}

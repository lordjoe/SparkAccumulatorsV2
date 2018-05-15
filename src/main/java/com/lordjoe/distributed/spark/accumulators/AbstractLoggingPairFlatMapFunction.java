package com.lordjoe.distributed.spark.accumulators;

import org.apache.spark.api.java.function.*;
import scala.*;

import java.io.Serializable;
import java.util.Iterator;

/**
 * ocom.lordjoe.distributed.spark.accumulators.AbstraceLoggingFunction
 * stand in for  PairFlatMapFunctio
 * superclass for defined functions that will log on first call making it easier to see
 * do work in doCall
 * User: Steve
 * Date: 10/23/2014
 */
public abstract class AbstractLoggingPairFlatMapFunction<T, K extends Serializable, V extends Serializable>
        extends AbstractLoggingFunctionBase implements PairFlatMapFunction<T, K, V> {


    /**
     * NOTE override doCall not this
     *
     * @param t
     * @return
     */
    @Override
    public final Iterator<Tuple2<K, V>> call(final T t) throws Exception {
        reportCalls();
        long startTime = System.nanoTime();
        Iterator<Tuple2<K, V>> ret = doCall(t);
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

    public abstract Iterator<Tuple2<K, V>> doCall(final T t) throws Exception;

}

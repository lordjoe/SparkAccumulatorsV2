package com.lordjoe.distributed.spark.accumulators;

import com.lordjoe.algorithms.*;
import com.lordjoe.testing.*;
import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.io.*;
import java.util.*;

/**
 * com.lordjoe.algorithms.MemoryUseAccumulator
 * Accululator to look at memory issues
 * to use create than call check to look at memory state
 * call saveBins() to save maximum memory use
 *
 * @author Steve Lewis
 * @date 5/28/2015
 */
public class MemoryUseAccumulator extends IAccumulator<MemoryUseAccumulator> {

    public static final String MEMORY_ACCUMULATOR_NAME = "MemoryUseAccumulator";

    public static final int ONE_K = 1025;
    public static final int MEG_40 = 500 * ONE_K * ONE_K; // 10 MB
    public static final int MAX_TRACKED_100_MEG_CHUNKS = (int)(20L * ONE_K * ONE_K * ONE_K / MEG_40 ); // up to 20 gb
    public static final int MEG_4 = 200 * ONE_K * ONE_K; // 1 MB
    public static final int MAX_TRACKED_40_MEG_CHUNKS =  (int)(20L * ONE_K * ONE_K * ONE_K / MEG_4 ); ; // up to 20 gb


    public static MemoryUseAccumulator empty() {
        return new MemoryUseAccumulator();
    }

    private transient long startAllocation;
    private transient long maxAllocated;

    private long maxHeap;
    private final int[] bins = new int[MAX_TRACKED_100_MEG_CHUNKS];
    private final int[] allocated = new int[MAX_TRACKED_40_MEG_CHUNKS];

    /**
     * Use static method empty
     */
    private MemoryUseAccumulator() {
        reset();
     }


    @Override
    public void reset() {
        startAllocation = MemoryTracker.usedBytes();
        maxHeap = startAllocation;
        maxAllocated = 0;
    }
        /**
         * given a value return it as 0
         * default behavior os th return the value itself
         *
         * @return
         */
    @Override
    public MemoryUseAccumulator asZero() {
        return   new MemoryUseAccumulator() ;
    }

    @Override
    public boolean isZero() {
        return maxAllocated == 0;
    }

    @Override
    public IAccumulator<MemoryUseAccumulator> copy() {
           return value();
    }

    @Override
    public void merge(AccumulatorV2<MemoryUseAccumulator, MemoryUseAccumulator> otherX) {
        MemoryUseAccumulator other = (MemoryUseAccumulator)otherX.value();
        add(other);
    }

    @Override
    public MemoryUseAccumulator value() {
        MemoryUseAccumulator ret = new MemoryUseAccumulator();
        ret.maxHeap = maxHeap;
        System.arraycopy(bins,0,ret.bins,0,bins.length);
        System.arraycopy(allocated,0,ret.allocated,0,allocated.length);
        return ret;
    }

    public int getBin(int bin) {
        return bins[bin];
    }

    public int[] getBins() {
        int[] ret = new int[bins.length];
        System.arraycopy(bins, 0, ret, 0, bins.length);
        return ret;
    }

    public int[] getAllocatedBins() {
        int[] ret = new int[allocated.length];
        System.arraycopy(allocated, 0, ret, 0, allocated.length);
        return ret;
    }

    public void check() {
        long current = MemoryTracker.usedBytes();
        long allocated = current - startAllocation;
        maxHeap = Math.max(maxHeap, current);
        maxAllocated = Math.max(maxAllocated, allocated);
    }


    public void saveBins() {
        int iBin = (int) (maxAllocated / MEG_4);
        iBin = Math.min(allocated.length - 1, iBin);
        allocated[iBin]++;
        iBin = (int) (maxHeap / MEG_40);
        iBin = Math.min(bins.length - 1, iBin);
        bins[iBin]++;
    }


    public void add(MemoryUseAccumulator added) {

        maxHeap = Math.max(maxHeap, added.maxHeap);
        for (int i = 0; i < bins.length; i++) {
            bins[i] += added.bins[i];
        }
        for (int i = 0; i < allocated.length; i++) {
            allocated[i] += added.allocated[i];
        }
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

        sb.append("Mem Use Max Allocation " + Long_Formatter.format(maxHeap));
        sb.append("\n");


        long index = MEG_40;
        for (int i = 0; i < bins.length; i++) {
            int bin = bins[i];
            if (bin > 0) {
                sb.append(Long_Formatter.format(index) + "\t" + Long_Formatter.format(bin));
                sb.append("\n");
            }
            index += MEG_40;
        }

        sb.append("Allocated\n");

        index = MEG_4;
        for (int i = 0; i < allocated.length; i++) {
            int bin = allocated[i];
            if (bin > 0) {
                sb.append(Long_Formatter.format(index) + "\t" + Long_Formatter.format(bin));
                sb.append("\n");
            }
            index += MEG_4;
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryUseAccumulator that = (MemoryUseAccumulator) o;

        return Arrays.equals(bins, that.bins);

    }

    @Override
    public int hashCode() {
        return bins != null ? Arrays.hashCode(bins) : 0;
    }
}

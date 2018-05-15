package com.lordjoe.distributed.spark.accumulators;

/**
 * com.lordjoe.distributed.spark.accumulators.CountedItem
 * User: Steve
 * Date: 8/7/2015
 */
import com.lordjoe.algorithms.*;

import java.io.*;

/**
 * convenient class to report results of machine use
 */
public class CountedItem implements Comparable<CountedItem>, Serializable {
    private final String m_Value;
    private final long m_Count;

    public CountedItem(final String pValue, final long pCount) {
        m_Value = pValue;
        m_Count = pCount;
    }

    public String getValue() {
        return m_Value;
    }

    public long getCount() {
        return m_Count;
    }

    /**
     * sort value - then count
     *
     * @param o  to compare
     * @return  standard comparison
     */
    @Override
    public int compareTo(final CountedItem o) {

        int ret = getValue().compareTo(o.getValue());
        if(ret != 0)
            return ret; // sort by name
        long count = getCount();
        long ocount = o.getCount();
        // High count first
        if (count != ocount)
            return count > ocount ? -1 : 1;
        return 0;
    }

    @Override
    public String toString() {
        return getValue() + ":" + Long_Formatter.format(getCount());
    }

}

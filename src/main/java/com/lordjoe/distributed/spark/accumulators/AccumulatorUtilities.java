package com.lordjoe.distributed.spark.accumulators;

import java.io.*;

/**
 * com.lordjoe.distributed.spark.accumulators.AccumulatorUtilities
 * Holds the only implementation of  ISparkAccumulators
 * also logging flag - this allows alternate implementations to be installed
 * User: Steve
 * Date: 8/7/2015
 */
public class AccumulatorUtilities implements Serializable {
   private static ISparkAccumulators onlyInstance;

    private static boolean functionsLoggedByDefault = true;

    public static boolean isFunctionsLoggedByDefault() {
        return functionsLoggedByDefault;
    }

    public static void setFunctionsLoggedByDefault(final boolean pFunctionsLoggedByDefault) {
        functionsLoggedByDefault = pFunctionsLoggedByDefault;
    }

    public static ISparkAccumulators getInstance() {
        return onlyInstance;
    }

    public static void setInstance(final ISparkAccumulators pOnlyInstance) {
        if(onlyInstance != null) {
            if(pOnlyInstance == onlyInstance)
                return;
            throw new IllegalStateException("onlyInstance cna only be set once");
        }
        onlyInstance = pOnlyInstance;
    }

    public static final double MILLISEC_IN_NANOSEC = 1000 * 1000;
     public static final double SEC_IN_NANOSEC = MILLISEC_IN_NANOSEC * 1000;
     public static final double MIN_IN_NANOSEC = SEC_IN_NANOSEC * 60;
     public static final double HOUR_IN_NANOSEC = MIN_IN_NANOSEC * 60;
     public static final double DAY_IN_NANOSEC = HOUR_IN_NANOSEC * 24;

    public static String formatNanosec(long timeNanosec) {
          if (timeNanosec < 10 * SEC_IN_NANOSEC)
              return String.format("%10.2f", timeNanosec / MILLISEC_IN_NANOSEC) + " msec";
          if (timeNanosec < 10 * MIN_IN_NANOSEC)
              return String.format("%10.2f", timeNanosec / SEC_IN_NANOSEC) + " sec";
          if (timeNanosec < 10 * HOUR_IN_NANOSEC)
              return String.format("%10.2f", timeNanosec / MIN_IN_NANOSEC) + " min";
          if (timeNanosec < 10 * DAY_IN_NANOSEC)
              return String.format("%10.2f", timeNanosec / HOUR_IN_NANOSEC) + " hour";
          return String.format("%10.2f", timeNanosec / DAY_IN_NANOSEC) + " days";
      }

}

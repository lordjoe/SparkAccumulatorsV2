package com.lordjoe.testing;

import java.io.*;
import java.text.*;

/**
 * com.lordjoe.utilities.ElapsedTimer
 *
 * org.systemsbiology.ElapsedTimer
  *   Helper class that can time operations
  *   Usege
  *    ElapsedTimer et = new ElapsedTimer()
  *    < Operation to time></>
  *    String timerMessage = et.formatElapsed("Operation 1 "); // adds time as a string
  *    et.reset(); // back to 0
  *    < Operation2 to time></>
  *    String timerMessage2 = et.formatElapsed("Operation 2"); // adds time as a string
  * @author Steve Lewis
   */


 public class ElapsedTimer implements Serializable {

        private long m_Start;

        public ElapsedTimer()
        {
             m_Start = System.currentTimeMillis();
        }

        /**
         * get when the operation started
         * @return  as above
         */
        public long getStart()
        {
            return m_Start;
        }

        /**
         * return start time to current
         */
        public void reset()
        {
             m_Start = System.currentTimeMillis();
        }

        /**
         * get time elapsed since start
         * @return  as above
          */
        public long getElapsedMillisec()
        {
            return  System.currentTimeMillis() - m_Start;
        }

        /**
         * return elapsed time as a string
         * @return  as above
         */
        public String formatElapsed( )
        {
            return formatElapsed("");
        }

        /**
         * return elapsed time as a string  with message included
          * @param message  !null message
         * @return  as above
         */
        public String formatElapsed(String message)
        {
             long elapsed = getElapsedMillisec();
            double elapsedSec = elapsed / 1000.0;
            if(elapsedSec < 1000 )
                 return message+ " in " +  formatDouble(elapsedSec,3 ) + " sec";
            if(elapsedSec < 10000 )
                 return message+ " in " +  formatDouble(elapsedSec / 60,3 ) + " min";
            return message + " in " +  formatDouble(elapsedSec / (60 * 60),3 ) + " hour";
         }

        /**
         * print elapsed time as a string  with message included on SYstem.out
         * @param message  !null message
         */
        public void showElapsed(String message)
        {
            showElapsed(message,System.out);

        }

        /**
         * print elapsed time as a string  with message included on out
           * @param out  !null PrintStream
         */
        public void showElapsed(PrintStream out)    {
            out.println(formatElapsed());
        }

        /**
         * print elapsed time as a string  with message included on  out
           * @param message  !null message
         * @param out  !null print stream
           */
        public void showElapsed(String message,PrintStream out)    {
            out.println(formatElapsed(message));
        }

        /**
          * print elapsed time as a string  with message included on out
            * @param out  !null PrintWriter
          */
        public void showElapsed(PrintWriter out)    {
            out.println(formatElapsed());
        }

        /**
         * print elapsed time as a string  with message included on  out
           * @param message  !null message
         * @param out  !null PrintWriter
           */
        public void showElapsed(String message,PrintWriter out)    {
            out.println(formatElapsed(message));
        }

    /**
       * convert a double into a String with a given precision
       * default double formatting is not very pretty
       *
       * @param in  non-null Double to convert
       * @param int positive precision
       * @return non-null formatted string
       */
      public static String formatDouble(Double in, int precision) {
          return (formatDouble(in.doubleValue(), precision));
      }

      /**
       * convert a double into a String with a given precision
       * default double formatting is not very pretty
       *
       * @param in  double to convert
       * @param int positive precision
       * @return non-null formatted string
       */
      public static String formatDouble(double in, int precision) {
          NumberFormat nf = NumberFormat.getInstance();
          nf.setMaximumFractionDigits(precision);
          return (nf.format(in));
      }

      /**
       * convert a double into a String with a given precision
       * default double formatting is not very pretty
       * This version defaults precision to 2
       *
       * @param in non-null Double to convert
       * @param in positive precision
       * @return non-null formatted string
       */
      public static String formatDouble(Double in) {
          return (formatDouble(in.doubleValue()));
      }


}

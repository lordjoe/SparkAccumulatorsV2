package com.lordjoe.distributed.spark.accumulators;



import com.lordjoe.algorithms.*;
import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * com.lordjoe.distributed.spark.accumulators.MachineUseAccumulator
 * track on which machine a an entry is made
 * User: Steve
 * Date: 11/24/2014
 */
public class MachineUseAccumulator   extends IAccumulator<MachineUseAccumulator> {

    public static MachineUseAccumulator empty() {
       return new MachineUseAccumulator();
    }


    // key is the machine MAC address
    private Map<String, Long> items = new HashMap<String, Long>();
    private long totalCalls;  // number function calls
    private long totalTime;   // call time in nanosec

    /**
     * Use static method empty
     */
     public MachineUseAccumulator() {
    }

    @Override
    public MachineUseAccumulator copy() {
        return new  MachineUseAccumulator(this);

    }

    /**
     * will be called to count use on a single machine
     */
    public MachineUseAccumulator(long n, long totalTime) {
        this();
        add(n, totalTime);
    }

    /**
     * copy constructor
     */
    public MachineUseAccumulator(MachineUseAccumulator copy) {
        this();
        items.putAll(copy.items);
        totalTime += copy.getTotalTime();
    }

    @Override
    public boolean isZero() {
        return totalCalls == 0;
    }

    public void add(long value, long totalT) {
        String macAddress =  getMacAddress();
        addEntry(macAddress, value);
        totalTime += totalT;
    }

    @Override
    public void merge(AccumulatorV2<MachineUseAccumulator, MachineUseAccumulator> other) {
        add((MachineUseAccumulator)other) ;
    }



    @Override
    public MachineUseAccumulator value() {
        return null;
    }

    protected void addEntry(String entry, long value) {
        long present = 0;
        if (items.containsKey(entry))
            present += items.get(entry);
        long value1 = value + present;
        items.put(entry, value1);
        totalCalls += value;
    }

    @Override
    public void reset() {
        items.clear();
        totalCalls = 0;
        totalTime = 0;
    }

    @Override
    public void add(MachineUseAccumulator added) {
        for (String t : added.items.keySet()) {
            long value = added.get(t);
            addEntry(t, value);
        }
        totalTime += added.getTotalTime();

    }

    public MachineUseAccumulator addx(MachineUseAccumulator added) {
        for (String t : added.items.keySet()) {
            long value = added.get(t);
            addEntry(t, value);
        }
        totalTime += added.getTotalTime();
        return this;
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

    /**
     * given a value return it as 0
     * default behavior os th return the value itself
     *
     * @return empty value
     */
    @Override
    public MachineUseAccumulator asZero() {
        return empty();
    }

    public long get(String item) {
        if (items.containsKey(item)) {
            return items.get(item);
        }
        return 0;
    }

    public long getTotalCalls() {
        if (totalCalls == 0)
            totalCalls = computeTotal();
        return totalCalls;
    }

    public long computeTotal() {
        long sum = 0;
        for (Long v : items.values()) {
            sum += v;
        }
        return sum;
    }


    public long getTotalTime() {
        return totalTime;
    }


    public int size() {
        return items.size();
    }

    /**
     * return counts with high first
     *
     * @return
     */
    public List<CountedItem> asCountedItems() {
        List<CountedItem> holder = new ArrayList<CountedItem>();
        for (String s : items.keySet()) {
            holder.add(new CountedItem(s, items.get(s)));
        }
        Collections.sort(holder);
        return holder;
    }

    public String getBalanceReport() {
        List<CountedItem> items = asCountedItems();
        int n = items.size();
        if (n < 2)
            return " variance 0"; // one machine is balanced

        long[] calls = new long[n];
        int index = 0;
        for (CountedItem item : items) {
            long count = item.getCount();
            calls[index++] = count;
        }
        return generateVariance(  calls);
    }

    public static String generateVariance( final long[] pCalls) {
        StringBuilder sb = new StringBuilder();
        double min = Double.MAX_VALUE;
        double max = 0;
        double sum = 0;
        double sumsq = 0;

        for (int i = 0; i < pCalls.length; i++) {
            long count = pCalls[i];
            min = Math.min(min, count);
            max = Math.max(max, count);
            sum += count;
            sumsq += count * count;

        }
        int n = pCalls.length;
        double average = sum / n;
        double sdsq = (n * sumsq - sum * sum) / (n * (n - 1));
        double sd = Math.sqrt(sdsq);
        //noinspection StringConcatenationInsideStringBufferAppend
        sb.append(" variance " + String.format("%6.3f", (sd / average)));
        return sb.toString();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" totalCalls:");
        long total1 = getTotalCalls();
        sb.append(Long_Formatter.format(total1));

        sb.append(" totalTime:");
        long totaltime = getTotalTime();
        sb.append(AccumulatorUtilities.formatNanosec(totaltime));

        sb.append(" machines:");
        sb.append(size());

        sb.append(getBalanceReport());

//        List<CountedItem> items = asCountedItems();
//        for (CountedItem item : items) {
//            sb.append(item.toString());
//            sb.append("\n");
//        }

        return sb.toString();
    }

    private transient static String macAddress;

     /**
      * identify the machine we are running on
      *
      * @return String representing a Mac address
      * @see http://www.mkyong.com/java/how-to-get-mac-address-in-java/
      */
     @SuppressWarnings("JavadocReference")
     public static String getMacAddress() {
         if (macAddress != null)
             return macAddress;
         InetAddress ip;
         try {

             ip = InetAddress.getLocalHost();
             // System.out.println("Current IP address : " + ip.getHostAddress());

             NetworkInterface network = NetworkInterface.getByInetAddress(ip);

             byte[] mac = network.getHardwareAddress();

             if (mac == null) {
                 mac = new byte[4];  // fake it if needed
                 mac[0] = 127;
                 mac[3] = 1;
             }


             StringBuilder sb = new StringBuilder();
             for (int i = 0; i < mac.length; i++) {
                 sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
             }
             macAddress = sb.toString();
             return macAddress;

         }
         catch (Exception e) {
             //throw new RuntimeException(e); // should never happen
             macAddress = "00-90-08-ab-ed-00";
             return macAddress;
         }
     }


}

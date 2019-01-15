package com.lordjoe.testing;

/**
 * com.lordjoe.testing.WordCountCaller
 * User: Steve
 * Date: 7/22/2018
 */
public class WordCountCaller {

    public static Class loadClass(String s)   {
        try {
            Class<?> aClass = Class.forName(s);
            System.out.println("loaded " + s);
            return aClass;
        }
        catch(ClassNotFoundException ex) {
            System.out.println("failed to Load " + s);
            return null;
        }


    }

    /**
     * testing that all references classes are available
     *
     * @param args
     */
    public static void main(String[] args) {
        loadClass("org.apache.spark.Accumulator");
        loadClass("org.apache.spark.AccumulatorParam");
        loadClass("org.apache.spark.SparkConf");
        loadClass("org.apache.spark.api.java.JavaPairRDD");
        loadClass("org.apache.spark.api.java.JavaRDD");
        loadClass("org.apache.spark.api.java.JavaSparkContext");
        loadClass("org.apache.spark.api.java.function.FlatMapFunction");
        loadClass("org.apache.spark.api.java.function.Function2");
        loadClass("org.apache.spark.api.java.function.PairFunction");
        loadClass("scala.Option");
        loadClass("scala.Tuple2");
        loadClass("java.io.Serializable");
         ExampleWordCount.main(args);
    }

}

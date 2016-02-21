package com.huntdreams.streaming.ch1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * SparkExample
 *
 * @author tyee.noprom@qq.com
 * @time 2/19/16 9:44 PM.
 */
public class SparkExample {

    public static void main(String[] args) {
        System.out.println("Creating spark configuration");
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("First java spark program");
        System.out.println("Creating spark context");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        String file = System.getenv("SPARK_HOME") + "/README.md";
        JavaRDD<String> logData = sparkContext.textFile(file);

        // Invoke filter operation on rdd
        Long numLines = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return true;
            }
        }).count();

        // Print the number of lines
        System.out.println("Number of lines in the dataset : " + numLines);
        sparkContext.close();
    }
}

package edu.yuli.task1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Get the number of lines in the file from local environment
 */
public class SparkNationalParkDemo {
    public static void main(String[] args) {
        System.out.println("Hello Spark!");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("nationalparks.csv");
        System.out.println("Number of lines in file = " + stringJavaRDD.count());
    }
}
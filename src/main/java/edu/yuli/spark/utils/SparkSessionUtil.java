package edu.yuli.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class SparkSessionUtil
 *
 * Created by yslabko on 01/10/2018.
 */
public class SparkSessionUtil {

    private static JavaSparkContext context;

    static {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
    }

    public static SparkSession getSession() {
        return getSession("Yuli's Ranking Spark App");
    }

    public static SparkSession getSession(String appName) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");


        SparkSession session = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        return session;
    }

    public static JavaSparkContext getContext(String appName) {
        if (context == null) {
            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName)
                    .setMaster("local[*]");

            context = new JavaSparkContext(sparkConf);
        }

        return context;
    }

    public static JavaSparkContext getContext() {
        return getContext("Yuli's spark context");
    }

    public static void closeSession() {
        getSession().close();
    }

    public static void closeContext() {
        getContext().close();
    }
}

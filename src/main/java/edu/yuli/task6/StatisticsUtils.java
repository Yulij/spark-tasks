package edu.yuli.task6;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;

import edu.yuli.spark.utils.SparkSessionUtil;

/**
 * Class StatisticsUtils.
 * Aggregates statistic methods for task 6
 *
 * Designed by yslabko
 *
 * Additional thanks to Irina Makarushko
 * for explanations and given solutions of statistic methods
 */
public class StatisticsUtils {
    // Apply a transform to get a random double RDD following `N(5.5, 2.25)` on interval [1, 10].
    // 3*Sigma = 4.5 -> Sigma=1.5 => Dispersion = 2.25
    private static final double MEDIAN = 5.5;
    private static final double DISPERSION = 2.25;
    private static final double MIN_SCORE = 1.0;
    private static final double MAX_SCORE = 10.0;
    private static final double SCORE_INTERVAL = 0.1;
    private static final int INTERVAL_COUNT = (int) ((MAX_SCORE - MIN_SCORE) / SCORE_INTERVAL);

    /**
     * Generate a random double RDD that contains count values drawn from the standard normal distribution `N(0, 1)`,
     * evenly distributed in 10 partitions [1, 10].
     *
     * @param count - distributed values count
     * @return javaDoubleRDD
     */
    private static JavaDoubleRDD getNormalDistributedRdd(int count) {
        JavaDoubleRDD standardNormal = normalJavaRDD(SparkSessionUtil.getContext(), count, (int) MAX_SCORE);
        return standardNormal.mapToDouble(x -> MEDIAN + Math.sqrt(DISPERSION) * x);
    }

    /**
     * Get normal distributed double values list
     *
     * @param valuesCount
     * @return List<Double>
     */
    public static List<Double> getNormalDistributedDouble(int valuesCount) {
        JavaDoubleRDD normal = getNormalDistributedRdd(valuesCount);
        return normal.take(valuesCount);
    }

    public static Vector getFrequencyVector(Dataset<Double> values) {
        double[] frequencies = new double[INTERVAL_COUNT];
        values.foreach(value -> {
            if (value > MIN_SCORE && value < MAX_SCORE) {
                int index = (int) ((value - MIN_SCORE) / SCORE_INTERVAL);
                frequencies[index] = frequencies[index] + 1;
            } else {
                frequencies[0] = frequencies[0] + 1;
            }
        });
        return Vectors.dense(frequencies);
    }

    public static Vector getTheoreticalFrequenciesVector(int valuesCount) {
        List<Double> theoreticalFrequencies = theoreticalFrequencies(valuesCount);
        double[] theoreticalFrequenciesArray = new double[theoreticalFrequencies.size()];
        for (int i = 0; i < theoreticalFrequencies.size(); i++) {
            theoreticalFrequenciesArray[i] = theoreticalFrequencies.get(i);
        }

        return Vectors.dense(theoreticalFrequenciesArray);
    }

    private static List<Double> theoreticalFrequencies(int valuesCount) {
        List<Double> result = new ArrayList<>();
        double middleInterval = MIN_SCORE + SCORE_INTERVAL / 2;
        for (int i = 0; i < INTERVAL_COUNT; i++) {
            double ti = (middleInterval - MEDIAN) / Math.sqrt(DISPERSION);
            middleInterval += SCORE_INTERVAL;
            double theoreticalValue = SCORE_INTERVAL * valuesCount * fi(ti) / MEDIAN;
            result.add(theoreticalValue);
        }
        return result;
    }

    private static double fi(double t) {
        return Math.exp(-(t * t) / 2) / (Math.sqrt(2 * Math.PI));
    }
}

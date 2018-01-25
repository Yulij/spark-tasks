package edu.yuli.task6;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;

import edu.yuli.spark.utils.SparkSessionUtil;

/**
 * Class StudentRankDemo
 *
 * Created by yslabko on 01/18/2018.
 */
public class StudentRankDemo {

    private static final int STUDENTS_COUNT = 1000;

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        List<Student> students = StudentGenerator.generateStudents(STUDENTS_COUNT);
        students.forEach(System.out::println);

        List<Double> ranks = students.stream()
                .map(Student::getRank)
                .collect(Collectors.toList());

        Vector rankFrequencies = StatisticsUtils.getFrequencyVector(ranks);
        Vector theoreticalRankFrequencies = StatisticsUtils.getTheoreticalFrequenciesVector(STUDENTS_COUNT);
        ChiSqTestResult testResult = Statistics.chiSqTest(rankFrequencies, theoreticalRankFrequencies);
        SparkSessionUtil.closeContext();
        Thread.sleep(2000);

        System.out.println("Chi squared summary:");
        System.out.println("Degrees of freedom\t\tStatistic\t\tpValue");
        System.out.printf("%10d%21.2f%15.5f%n", testResult.degreesOfFreedom(), testResult.statistic(), testResult.pValue());
        System.out.println("Conclusion: " + testResult.nullHypothesis());
    }
}

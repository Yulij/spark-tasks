package edu.yuli.task6;

import java.util.List;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import edu.yuli.spark.utils.SparkSessionUtil;

/**
 * Class StudentRankDemo
 *
 * Created by yslabko on 01/18/2018.
 */
public class StudentRankDemo {

    private static final int STUDENTS_COUNT = 1000;

    public static void main(String[] args) throws InterruptedException {
        List<Student> students = StudentGenerator.generateStudents(STUDENTS_COUNT);

        Encoder<Student> studentEncoder = Encoders.bean(Student.class);
        Dataset<Student> studentsDs = SparkSessionUtil.getSession().createDataset(
                students,
                studentEncoder
        );

        Encoder<Double> stringEncoder = Encoders.DOUBLE();
        Dataset<Double> ranksDf = studentsDs.map(
                Student::getRank,
                stringEncoder);

        Vector rankFrequencies = StatisticsUtils.getFrequencyVector(ranksDf);
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

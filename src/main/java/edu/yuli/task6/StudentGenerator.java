package edu.yuli.task6;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;

/**
 * Class StudentGenerator
 *
 * Created by yslabko on 01/18/2018.
 */
public class StudentGenerator {
    public static List<Student> generateStudents(int valuesCount) {
        List<Double> ranks = StatisticsUtils.getNormalDistributedDouble(valuesCount);
        List <Student> list = new ArrayList<>();
        for (int i = 1; i <= valuesCount; i++) {
            list.add(generateStudent(i, ranks.get(i-1)));
        }

        return list;
    }

    private static Student generateStudent(int i, double score) {
        Student student = new Student();
        student.setId(i);
        student.setName("Name" + i);
        student.setSurname("Surname" + i);
        student.setRank(score);

        return student;
    }
}

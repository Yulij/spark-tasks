package edu.yuli.task6;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Class Student
 *
 * Created by yslabko on 01/18/2018.
 */
@Data
@AllArgsConstructor @NoArgsConstructor
public class Student {
    private long id;
    private String name;
    private String surname;
    private double rank;
}

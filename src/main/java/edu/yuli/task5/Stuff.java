package edu.yuli.task5;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Class Stuff
 * start - timestamp
 * from - from city
 * to - destination city
 * duration - duration in days
 * region - same to
 * position - stuff position
 *
 * Created by yslabko on 01/10/2018.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stuff {
    private long start;
    private String from;
    private String to;
    private int duration;
    private String region;
    private String position;
}

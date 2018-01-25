package edu.yuli.task5.utils;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import edu.yuli.task5.Stuff;

/**
 * Class StuffGenerator
 *
 * Created by yslabko on 01/08/2018.
 */
public class StuffGenerator {

    private static final int SECONDS_IN_YEAR = 524_160;
    private static final int DAYS_IN_3_MONTHS = 90;

    private static String[] cities = {"Minsk", "Gomel", "Moskow", "Vitebsk", "Vinitsa", "Brest", "Vilnus", "Warshava", "Krakov", "Lvov", "Berlin", "Paris"};
    private static String[] positions = {"QA", "BA", "D", "SD", "TL" , "AL", "PM", "CEO", "CTO", "CAO"};

    public static List<Stuff> generateStuffList(int count) {
        List <Stuff> list = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            list.add(generateStuff());
        }

        return list;
    }

    private static Stuff generateStuff() {
        Stuff stuff = new Stuff();
        LocalDateTime dateTime = LocalDateTime.now().minusMinutes(ThreadLocalRandom.current().nextInt(1, SECONDS_IN_YEAR));
        Date date = new Date(Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant()).getTime());
        stuff.setStart(date.getTime());
        stuff.setFrom(cities[getInt(0, cities.length-1)]);
        stuff.setTo(cities[getInt(0, cities.length-1)]);
        stuff.setDuration(getInt(1, DAYS_IN_3_MONTHS));
        stuff.setRegion(stuff.getTo());
        stuff.setPosition(positions[getInt(0, positions.length-1)]);

        return stuff;
    }

    private static int getInt(int start, int end) {
        return ThreadLocalRandom.current().nextInt(start, end + 1);
    }
}

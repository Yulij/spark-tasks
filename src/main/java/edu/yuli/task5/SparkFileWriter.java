package edu.yuli.task5;

import java.util.Date;
import java.util.List;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.yuli.spark.utils.SparkSessionUtil;
import edu.yuli.task5.utils.StuffGenerator;

/**
 * Class SparkFileWriter
 *
 * Created by yslabko on 01/10/2018.
 */
public class SparkFileWriter {
    public static void main(String[] args) {
//        writeData(2_000_000);
        readDataFromFile();
    }

    private static void writeData(int amount) {
        List<Stuff> stuffs = StuffGenerator.generateStuffList(amount);
        writeDataToFiles(stuffs);
    }

    private static void readDataFromFile() {
//        read("stuff.parquet");
        read("stuff.csv");
    }

    private static void read(String fileName) {
        SparkSession session = SparkSessionUtil.getSession();
        DataFrameReader reader = session.read();

        if (fileName.contains(".csv")) {
            reader.format("csv")
                    .option("inferSchema", "true")
                    .option("header", "true");
        }

        long st = new Date().getTime();
        Dataset<Row> stuffDF = reader.load("src/main/resources/" + fileName);
        stuffDF.printSchema();
        long et = new Date().getTime();
        System.out.println("Read time from file: " + fileName + " - " + (et - st) + " ms");
    }

    private static void writeDataToFiles(List<Stuff> stuffs) {
        SparkSession session = SparkSessionUtil.getSession();
        Dataset<Row> stuffDF = session.createDataFrame(stuffs, Stuff.class);
//        stuffDF.coalesce(1)
//                .write()
//                .format("parquet")
//                .parquet("stuff.parquet");

        stuffDF.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("inferSchema","true")
                .option("header", "true")
                .save("stuff.csv");
    }
}

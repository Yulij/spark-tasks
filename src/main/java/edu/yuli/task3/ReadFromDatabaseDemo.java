package edu.yuli.task3;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.format_number;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.yuli.spark.utils.SparkSessionUtil;

/**
 * Class RankingTask
 *
 * Created by yslabko on 01/08/2018.
 */
public class ReadFromDatabaseDemo {

    public static void main(String[] args) throws AnalysisException {
        ReadFromDatabaseDemo demo = new ReadFromDatabaseDemo();
        Dataset<Row> df = demo.readFromMysql(SparkSessionUtil.getSession());
        getRankByFunctions(df);
    }

    private static void getRankByFunctions(Dataset<Row> df) {
        df.select( col("CATEGORY"), col("BRAND"), col("COST"))
                .groupBy(col("CATEGORY"), col("BRAND"))
                .sum("COST")
                .orderBy("CATEGORY")
                .orderBy(desc("sum(COST)"))
                .withColumn("sum(COST)", regexp_replace(format_number(col("sum(COST)"), 2), ",", " "))
                .show();
    }

    private Dataset<Row> readFromMysql(SparkSession spark) {
        ResourceBundle rb = ResourceBundle.getBundle("db");
        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", rb.getString("driver"))
                .option("url", rb.getString("url"))
                .option("dbtable", "ORDER_ITEM_VIEW")
                .option("user", rb.getString("user"))
                .option("password", rb.getString("password"))
                .load();

        if (jdbcDF == null ) {
            System.out.println("No dataframe initialized");
        }

        return jdbcDF;
    }
}

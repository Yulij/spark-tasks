package edu.yuli.task4;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class RankingTask
 *
 * Created by yslabko on 01/08/2018.
 */
public class RankingTask {

    public static void main(String[] args) throws AnalysisException {
        RankingTask demo = new RankingTask();
        SparkConf sparkConf = new SparkConf()
                .setAppName("Yuli's Ranking Spark App")
                .setMaster("local[*]");

        SparkSession session = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config(sparkConf)
                .getOrCreate();
        Dataset<Row> df = demo.readFromMysql(session);
        getRankBySQL(df, session);
    }

    private static void getRankBySQL(Dataset<Row> df, SparkSession session) throws AnalysisException {
        df.createGlobalTempView("shop");
        Dataset<Row> sqlDF = session.sql(
                "SELECT \n" +
                        "  s.CATEGORY as Category," +
                        "  s.BRAND as Brand," +
                        "  s.TOTAL as Revenue," +
                        "  rank() OVER (PARTITION BY s.CATEGORY ORDER BY s.TOTAL DESC) AS Top \n " +
                        "FROM (SELECT  " +
                        "  CATEGORY," +
                        "  BRAND," +
                        "  sum(COST) AS TOTAL " +
                        "  FROM global_temp.shop\n " +
                        "GROUP BY CATEGORY, BRAND) AS s\n "
                        + "HAVING Top <= 3"
        );
        sqlDF.show();
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

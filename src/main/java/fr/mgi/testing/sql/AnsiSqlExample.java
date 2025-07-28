package fr.mgi.testing.sql;

import org.apache.spark.sql.SparkSession;

// https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
public class AnsiSqlExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 ANSI SQL Example")
                .master("local[*]")
                .config("spark.sql.ansi.enabled", "false")
                .getOrCreate();

        spark.sql("SELECT 2147483647 + 1 AS overflowed_value").show();

        spark.sql("SELECT CAST('a' AS INT)").show();

        spark.sql("SELECT CAST(DATE'2020-01-01' AS INT)").show();

        // Arrêter la session Spark
        spark.stop();
    }
}
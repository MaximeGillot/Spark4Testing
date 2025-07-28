package fr.mgi.testing.sql.functions;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

//https://issues.apache.org/jira/browse/SPARK-52016
public class DayNameExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 DAYNAME Example")
                .master("local[*]")
                .getOrCreate();

        // Créer un DataFrame avec des dates
        Dataset<Row> dates = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create("2023-01-15"),
                        RowFactory.create("2023-02-20"),
                        RowFactory.create("2023-03-10")
                ),
                DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, false)
                ))
        );

        // Convertir la colonne "date" en type Date et ajouter une colonne avec le nom du jour
        Dataset<Row> result = dates
                .withColumn("date", functions.to_date(functions.col("date")))
                .withColumn("day_name", functions.dayname(functions.col("date")));

        // Afficher le résultat
        result.show(false);

        // Arrêter la session Spark
        spark.stop();
    }
}
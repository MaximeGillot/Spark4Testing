package fr.mgi.testing.sql.functions;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;


public class ListAggExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 LISTAGG Example")
                .master("local[*]")
                .getOrCreate();

        // Créer un DataFrame avec des données
        Dataset<Row> data = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create("A", "val1"),
                        RowFactory.create("A", "val2"),
                        RowFactory.create("B", "val3"),
                        RowFactory.create("B", "val4"),
                        RowFactory.create("B", "val4")
                ),
                DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("group", DataTypes.StringType, false),
                        DataTypes.createStructField("value", DataTypes.StringType, false)
                ))
        );

        // Utiliser LISTAGG pour agréger les valeurs par groupe
        Dataset<Row> result = data.groupBy("group")
                .agg(
                        functions.listagg(functions.col("value")).as("aggregated_values"),
                        functions.listagg_distinct(functions.col("value")).as("aggregated_values"),
                        functions.listagg(functions.col("value"), functions.lit(",")).as("aggregated_values"),
                        functions.listagg_distinct(functions.col("value"), functions.lit(";")).as("aggregated_values")
                );


        // Afficher le résultat
        result.show(false);

        // Arrêter la session Spark
        spark.stop();
    }
}
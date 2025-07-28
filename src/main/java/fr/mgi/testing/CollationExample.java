package fr.mgi.testing;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

// list collation intéressante: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/collations
public class CollationExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark Collation Example")
                .master("local[*]")
                .getOrCreate();

        // Créer un DataFrame avec des chaînes de caractères
        Dataset<Row> data = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create("Ällemand"),
                        RowFactory.create("apple"),
                        RowFactory.create("banana"),
                        RowFactory.create("éclair"),
                        RowFactory.create("Éclair"),
                        RowFactory.create("zèbre"),
                        RowFactory.create("大家好")
                ),
                DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("word", DataTypes.StringType, false)
                ))
        );

        // Tri avec la collation par défault utf-8
        System.out.println("Tri avec collation utf8 :");
        data.orderBy(functions.col("word")).show(false);

        // Tri avec la collation française
        System.out.println("Tri avec la collation française (fr) :");
        data.orderBy(functions.collate(functions.col("word"), "fr")).show(false);

        // Tri avec la collation allemande
        System.out.println("Tri avec la collation allemande (de) :");
        data.orderBy(functions.collate(functions.col("word"), "de")).show(false);

        // Tri avec la collation allemande
        System.out.println("Tri avec la collation chinoise (zh) :");
        data.orderBy(functions.collate(functions.col("word"), "zh")).show(false);


        // Arrêter la session Spark
        spark.stop();
    }
}
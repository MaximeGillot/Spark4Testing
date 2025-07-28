package fr.mgi.testing.sql.functions;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

// Add a `try_url_decode` function that performs the same operation as `url_decode`, but returns a NULL value instead of raising an error if the decoding cannot be performed.
public class TryUrlDecodeExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 try_url_decode Example")
                .master("local[*]")
                .getOrCreate();

        // Créer un DataFrame avec des URL encodées
        Dataset<Row> urls = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create("https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue"),
                        RowFactory.create("https%3A%2F%2Ftest.com%2Fsearch%3Fq%3Dspark"),
                        RowFactory.create("invalid%url%encoding")
                ),
                DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("encoded_url", DataTypes.StringType, false)
                ))
        );

        // Décoder les URL encodées en utilisant try_url_decode
        Dataset<Row> result = urls.withColumn("decoded_url", functions.try_url_decode(functions.col("encoded_url")));

        // Afficher le résultat
        result.show(false);

        // Arrêter la session Spark
        spark.stop();
    }
}
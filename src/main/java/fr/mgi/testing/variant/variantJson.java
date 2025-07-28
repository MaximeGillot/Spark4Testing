package fr.mgi.testing.variant;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class variantJson {
    // https://issues.apache.org/jira/browse/SPARK-45827
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark JSON Reader Example")
                .master("local[*]")
                .getOrCreate();

        // Définir un schéma avec une colonne de type Variant
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("data", DataTypes.StringType); // La colonne sera convertie en Variant

        // Créer un DataFrame avec des données hétérogènes
        Dataset<Row> data = spark.createDataFrame(
                java.util.Arrays.asList(
                        RowFactory.create(1, "{\"a\":1,\"b\":0.8}"),
                        RowFactory.create(2, "{\"a\":2,\"b\":0.5}")
                ),
                schema
        );

        // Afficher le schéma du DataFrame
        data.printSchema();

        data.show();

        Dataset<Row> t = data.withColumn("valueA", functions.parse_json(functions.col("data")));

        t.printSchema();
        t.show();

        t.withColumn("valueA", functions.variant_get(functions.col("valueA"),"$.a", "string"))

                .select("id", "valueA")
                .show();

        // Afficher le contenu du DataFrame
        //   jsonData.select(new Column("people.id")) .show();

        spark.stop();

    }
}

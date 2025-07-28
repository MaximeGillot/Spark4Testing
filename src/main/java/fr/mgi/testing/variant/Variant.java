package fr.mgi.testing.variant;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class Variant {

        // https://issues.apache.org/jira/browse/SPARK-45827
        public static void main(String[] args) {
            // Créer une SparkSession
            SparkSession spark = SparkSession.builder()
                    .appName("Spark 4 Variant Data Type Example")
                    .master("local[*]")
                    .getOrCreate();

            // Définir un schéma avec une colonne de type Variant
            StructType schema = new StructType()
                    .add("id", DataTypes.IntegerType)
                    .add("data", DataTypes.VariantType); // La colonne sera convertie en Variant

            // Créer un DataFrame avec des données hétérogènes
            Dataset<Row> data = spark.createDataFrame(
                    java.util.Arrays.asList(
                            RowFactory.create(1, "texte simple"),
                            RowFactory.create(2, 12345), // Entier
                            RowFactory.create(3, true), // Booléen
                            RowFactory.create(4, java.util.Arrays.asList("valeur1", "valeur2")) // Liste
                    ),
                    schema
            );

            // Afficher le DataFrame
            data.show(false);

        }
}

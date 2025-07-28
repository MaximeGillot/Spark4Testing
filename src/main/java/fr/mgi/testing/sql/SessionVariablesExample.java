package fr.mgi.testing.sql;

import org.apache.spark.sql.SparkSession;

public class SessionVariablesExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark Session Variables Example")
                .master("local[*]")
                .config("spark.sql.session.timeZone", "UTC") // Exemple de configuration de variable de session
                .getOrCreate();

        // Définir une variable de session
        spark.sql("SET my_variable = 'Hello, Spark!'");

        // Récupérer et afficher la valeur de la variable de session
        spark.sql("SET my_variable").show(false);

        // Utiliser la variable de session dans une requête SQL
        spark.sql("SELECT ${my_variable} AS session_variable_value").show(false);

        // Arrêter la session Spark
        spark.stop();
    }
}
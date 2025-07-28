package fr.mgi.testing.connect;
import org.apache.spark.sql.SparkSession;

public class SparkConnectTest {
    public static void main(String[] args) {
        // Créer une SparkSession en utilisant Spark Connect
        SparkSession spark = SparkSession.builder()
                .appName("Spark Connect Example")
               // .master("local[*]") // Utilisez "local[*]" pour le mode local ou spécifiez un cluster
                .remote("spark://192.168.1.12:7077") // Remplacez par l'URL de votre Spark Connect
                .getOrCreate();

        // Exemple d'exécution d'une requête SQL
        spark.sql("SELECT 'Bonjour Spark Connect!' AS message").show();

        // Arrêter la session Spark
        spark.stop();
    }

}

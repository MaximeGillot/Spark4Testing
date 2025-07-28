package fr.mgi.testing.sql.functions;

import org.apache.spark.sql.SparkSession;

// https://issues.apache.org/jira/browse/SPARK-52016
public class CurrentUserSessionUserExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 CURRENT_USER and SESSION_USER Example")
                .master("local[*]")
                .getOrCreate();

        /*
        CURRENT_USER inside a routine should return security definer of a routine, e.g. owner identity
        SESSION_USER inside a routine should return connected user.
        */
        spark.sql("SELECT CURRENT_USER() AS current_user, SESSION_USER() AS session_user")
                .show(false);

        // Arrêter la session Spark
        spark.stop();
    }
}
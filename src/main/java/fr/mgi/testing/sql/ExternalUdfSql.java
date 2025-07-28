package fr.mgi.testing.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ExternalUdfSql {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 ANSI SQL Example")
                .master("local[*]")
                .getOrCreate();

        spark.sql("CREATE FUNCTION blue()\n" +
                "  RETURNS STRING\n" +
                "  COMMENT 'Blue color code'\n" +
                "  LANGUAGE SQL\n" +
                "  RETURN '0000FF'");

        spark.sql("SELECT blue()").show();

        spark.sql("CREATE FUNCTION to_hex(x INT COMMENT 'Any number between 0 - 255')\n" +
                "  RETURNS STRING\n" +
                "  COMMENT 'Converts a decimal to a hexadecimal'\n" +
                "  CONTAINS SQL DETERMINISTIC\n" +
                "  RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0)");

        spark.sql("SELECT to_hex(id) FROM range(15)").show();

        spark.sql("SELECT to_hex(id) FROM range(15)").explain();

        // Arrêter la session Spark
        spark.stop();
    }
}
package fr.mgi.testing;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class XmlReader {


    // https://spark.apache.org/docs/4.0.0/sql-data-sources-xml.html
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark 4 Variant Data Type Example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> pom = spark.read().option("rowTag", "dependency").xml("pom.xml");

        pom.printSchema();

        pom.show(false);

        // Alternatively, a DataFrame can be created for an XML dataset represented by a Dataset[String]
        List<String> xmlData = Collections.singletonList(
                "<person>" +
                        "<name>maxgillo</name><job>Developer</job><age>28</age>" +
                        "</person>");

        Dataset<String> otherPeopleDataset = spark.createDataset(Lists.newArrayList(xmlData),
                Encoders.STRING());

        Dataset<Row> otherPeople = spark.read()
                .option("rowTag", "person")
                .xml(otherPeopleDataset);

        otherPeople.show();


        otherPeople.write()
                .format("xml")
                .option("rootTag", "peoples")
                .option("rowTag", "person")
                .mode("overwrite")
                .save("src/main/resources/xml/people.xml");



    }

}

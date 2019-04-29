package com.rahul.hadoop.fileformats.orc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrcFileWriter {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("OrcFileWriter").getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read().format("csv")
                .option("header", "true").option("inferSchema", "true")
                .load("/home/rahulbhatia/Rahul_Bhatia/intellij_workspace/HadoopFileFormats/src/main/resources/userdata.csv");

        dataFrame.show();
        dataFrame.write().mode("overwrite").orc("userdata.orc");
    }
}
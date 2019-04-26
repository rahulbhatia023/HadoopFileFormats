package com.rahul.hadoop.fileformats.orc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class OrcFileWriter {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("OrcFileWriter").getOrCreate();
        StructType schema = new StructType()
                .add("id", "int")
                .add("firstName", "string")
                .add("lastName", "string");

        Dataset<Row> dataFrame = sparkSession.read()
                .option("header", "true").option("inferSchema", "true")
                .schema(schema)
                .csv("C:\\Rahul_Bhatia\\intellij_workspace\\HadoopFileFormats\\src\\main\\resources\\userdata.csv");

        dataFrame.show();
        //dataFrame.write().orc("userdata.csv");
    }
}

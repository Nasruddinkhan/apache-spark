package com.mypractice.project.two;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCsvSchema {
	public  void printInferCsvSchema() {
		SparkSession sparkSession = new SparkSession.Builder().appName("Complex Cvs to Dataframe").master("local")
				.getOrCreate();
		Dataset<Row> ds = sparkSession.read().format("csv")
				.load("src/main/resources/product.txt");
		System.out.println("The dataFrame content");
		ds.show(7, 15);
		System.out.println("The dataFreame schema");
		ds.printSchema();
	}
}

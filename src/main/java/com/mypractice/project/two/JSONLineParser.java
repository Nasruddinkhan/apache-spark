package com.mypractice.project.two;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLineParser {
	public void parseJsonLine() {
		SparkSession spark = SparkSession.builder().appName("Json line to dataFreame")
							  .master("local")
							  .getOrCreate();
		Dataset<Row> ds = spark.read().format("json").load("src/main/resources/simple.json");
		ds.show(5,150);
		ds.printSchema();
		
		Dataset<Row> ds1 = spark.read().format("json")
				.option("multiline", true)
				.load("src/main/resources/multiline.json");
		ds1.show(5,150);
		ds1.printSchema();
	}
}

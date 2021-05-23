package com.mypractice.project.one;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.File;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CsvToMySql {
	public static void main(String[] args) {

		System.out.println(new File("src/main/resources/data.txt"));
		// Create SparkSession
		SparkSession sparkSession = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();
		// get Data
		Dataset<Row> df = sparkSession.read().format("csv").option("header", true).load("src/main/resources/data.txt");
		df = df.withColumn("full_name", concat(df.col("last_name"), lit(","), df.col("first_name")));
		df = df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").desc());
		df.show();
		String url = "jdbc:mysql://localhost:3306/apache_spark";
		Properties properties = new Properties();
		properties.setProperty("driver", "com.mysql.jdbc.Driver");
		properties.setProperty("user", "root");
		properties.setProperty("password", "root");
		df.write()
			.mode(SaveMode.Overwrite)
			.jdbc(url, "emp", properties);;

	
	}
}

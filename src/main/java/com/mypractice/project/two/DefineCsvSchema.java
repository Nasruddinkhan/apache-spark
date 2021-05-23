package com.mypractice.project.two;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCsvSchema {

	public void printDefineSchema() {

		SparkSession sparkSession = SparkSession.builder().appName("Complex cvs to Dataframe").master("local")
				.getOrCreate();
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.IntegerType, false),
						DataTypes.createStructField("product_id", DataTypes.IntegerType, true),
						DataTypes.createStructField("item_name", DataTypes.StringType, false),
						DataTypes.createStructField("published_on", DataTypes.DateType, true),
						DataTypes.createStructField("url", DataTypes.StringType, false)});
		
		Dataset<Row> ds = sparkSession.read().format("csv")
				.option("header", "true")
				.option("multiline", true)
				.option("sep", ";")
				.option("quote", "^")
				.option("dateFormat", "M/d/y")
				.schema(schema)
				.load("src/main/resources/product.txt");
		System.out.println("The dataFrame content");
		ds.show(5, 15);
		System.out.println("The dataFreame schema");
		ds.printSchema();
	}
}

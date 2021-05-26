package com.mypractice.project.three;

import static org.apache.spark.sql.functions.*;

import java.time.LocalDateTime;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.LocalDate;

public class Application {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("combine two dataset together").master("local")
				.getOrCreate();

		Dataset<Row> firstDataset = buildFirstDataFrame(sparkSession);
		firstDataset.printSchema();
		firstDataset.show(50);

		Dataset<Row> secondDataFrame = buildSecondDataFrame(sparkSession);
		secondDataFrame.printSchema();
		secondDataFrame.show(10);

		combineDataframes(firstDataset, secondDataFrame);

	}

	private static void combineDataframes(Dataset<Row> firstDataset, Dataset<Row> secondDataFrame) {
		Dataset<Row> combineDf = firstDataset.unionByName(secondDataFrame);
		combineDf.show(500);
		combineDf.printSchema();
		System.out.println("Application.combineDataframes() rows [" + combineDf.count() + "]");

		//combineDf = combineDf.repartition(5);
		combineDf = combineDf.repartition(5);
		Partition[] partitions = combineDf.rdd().partitions();
		System.out.println("Total number of Partitions: " + partitions.length);
	}

	private static Dataset<Row> buildSecondDataFrame(SparkSession sparkSession) {
		Dataset<Row> df = sparkSession.read().format("csv").option("multiline", true).option("header", true)
				.load("src/main/resources/philadelphia_recreations.csv");
		// df = df.filter("lower(USE_) like '%park%' ");
		df = df.filter(lower(df.col("USE_")).like("%park%"))
				.withColumn("park_id", concat(lit("phil"), df.col("OBJECTID")))
				.withColumnRenamed("ASSET_NAME", "park_name").withColumn("city", lit("philadelphia"))
				.withColumnRenamed("ADDRESS", "address").withColumn("add_date", lit(LocalDateTime.now().toString()))
				.withColumn("has_playground", lit("UNKNOWN")).withColumnRenamed("ZIPCODE", "zipcode")
				.withColumnRenamed("ACREAGE", "land_in_acres").withColumn("geoX", lit("UNKNOWN"))
				.withColumn("geoY", lit("UNKNOWN")).drop("OBJECTID").drop("SITE_NAME").drop("TYPE").drop("USE_")
				.drop("ALLIAS").drop("SQ_FEET").drop("CHRONOLOGY").drop("NOTES").drop("DATE_EDITED").drop("DESCRIPTION")
				.drop("OCCUPANT").drop("TENANT").drop("CHILD_OF").drop("LABEL").drop("EDITED_BY");

		return df;
	}

	private static Dataset<Row> buildFirstDataFrame(SparkSession sparkSession) {
		Dataset<Row> df = sparkSession.read().format("json").option("multiline", true)
				.load("src/main/resources/durham-parks.json");
		new LocalDate();
		df = df.withColumn("park_id", concat(df.col("datasetid"), lit(","), df.col("fields.objectid")))
				.withColumn("park_name", df.col("fields.park_name")).withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("add_date", lit(LocalDateTime.now().toString()))
				.withColumn("has_playground", df.col("fields.playground")).withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1)).drop("fields").drop("geometry")
				.drop("record_timestamp").drop("recordid").drop("datasetid");
		return df;
	}
}

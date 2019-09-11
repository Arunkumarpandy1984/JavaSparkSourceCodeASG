/*
Java Spark Sample Source code Accessing Amazon S3 Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/

package com.java.lineage.s3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DF_S3_Combinations {

	//  we have a issue in 10.1 while computing lineage for the following 4 functions, and the fix for the issue is available in 10.2 version of Java spark Plugin.
	/*
	public void csvtoparquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF = spark.read().format("csv").load("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/emp.csv");
		userDF.write().format("parquet").save("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/empTarget");
		spark.close();
	}

	public void orctoparquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF2 = spark.read().format("orc").load("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/students.orc");
		userDF2.write().format("parquet").save("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/studentsTarget");
		spark.close();
	}

	public void jsontoparquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF3 = spark.read().option("multiLine", "true").format("json")
				.load("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/people.json");
		userDF3.write().format("parquet").save("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/peopleTarget");
		spark.close();
	}

	public void parquettoorc() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF4 = spark.read().format("parquet").load("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/rubiks.parquet");
		userDF4.write().format("orc").save("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/rubiksTarget");
		spark.close();
	}
	*/
	public void parquettoparquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF5 = spark.read().parquet("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/rubiks.parquet");
		userDF5.write().parquet("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/rubiksTarget1");
		spark.close();
	}

	public void texttotext() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF6 = spark.read().text("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/rddSource.txt");
		userDF6.write().text("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/rddSourceTarget1");
		spark.close();
	}

	public void jsontojson() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF7 = spark.read().option("multiLine", "true")
				.json("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/people.json");
		userDF7.write().json("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/peopleTarget_01");
		spark.close();
	}

	public void csvtocsv() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF8 = spark.read().option("multiLine", "true").csv("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/emp.csv");
		userDF8.write().csv("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/empTarget01");
		spark.close();
	}

	public static void main(String[] args) {
		/*
		new DF_S3_Combinations().csvtoparquet();
		new DF_S3_Combinations().orctoparquet();
		new DF_S3_Combinations().jsontoparquet();
		new DF_S3_Combinations().parquettoorc();
		*/
		new DF_S3_Combinations().parquettoparquet();
		new DF_S3_Combinations().texttotext();
		new DF_S3_Combinations().jsontojson();
		new DF_S3_Combinations().csvtocsv();

	}
}

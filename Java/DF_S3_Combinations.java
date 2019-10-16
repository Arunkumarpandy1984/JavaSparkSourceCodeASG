/*
Java Spark Sample Source code Accessing Amazon S3 Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/

package com.java.lineage.s3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DF_S3_Combinations {

	public void parquettoparquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF5 = spark.read().parquet("s3a://asg-ida-dev-versions/testFiles/JavaSpark/Source/rubiks.parquet");
		userDF5.write().parquet("s3a://asg-ida-dev-versions/testFiles/JavaSpark/JavaTarget/rubiksTarget1");
		spark.close();
	}

	public void texttotext() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF6 = spark.read().text("s3a://asg-ida-dev-versions/testFiles/JavaSpark/Source/rddSource.txt");
		userDF6.write().text("s3a://asg-ida-dev-versions/testFiles/JavaSpark/JavaTarget/rddSourceTarget1");
		spark.close();
	}

	public void jsontojson() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF7 = spark.read().option("multiLine", "true")
				.json("s3a://asg-ida-dev-versions/testFiles/JavaSpark/Source/people.json");
		userDF7.write().json("s3a://asg-ida-dev-versions/testFiles/JavaSpark/JavaTarget/peopleTarget_01");
		spark.close();
	}

	public void csvtocsv() {
		SparkSession spark = SparkSession.builder().appName("Java Spark AWS S3 basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> userDF8 = spark.read().option("multiLine", "true").csv("s3a://asg-ida-dev-versions/testFiles/JavaSpark/Source/emp.csv");
		userDF8.write().csv("s3a://asg-ida-dev-versions/testFiles/JavaSpark/JavaTarget/empTarget01");
		spark.close();
	}

	public static void main(String[] args) {

		new DF_S3_Combinations().parquettoparquet();
		new DF_S3_Combinations().texttotext();
		new DF_S3_Combinations().jsontojson();
		new DF_S3_Combinations().csvtocsv();

	}
}

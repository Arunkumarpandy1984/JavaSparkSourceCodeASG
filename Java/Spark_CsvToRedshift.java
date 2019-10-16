/*
Java Spark Sample Source code Accessing Amazon Redshift Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/


package com.java.lineage.redshift;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Spark_CsvToRedshift {

	public void doReadCsvToRedshift() {
		SparkSession spark = SparkSession.builder().appName("Java Spark Redshift basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		
		Dataset<Row> jdbcDF_r5 = null;
		jdbcDF_r5 = spark.read().csv("s3a://asg-ida-dev-versions/testFiles/JavaSpark/Source/emp.csv");
		//jdbcDF = spark.read().csv("C:/RedShift/Raghav/LineageSourceFiles/user/root/java/QASource/spark.csv");
		
		jdbcDF_r5.write().mode(SaveMode.Overwrite)
			.format("jdbc")	
			.option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world")
			.option("driver","com.amazon.redshift.jdbc.Driver")
			.option("dbtable", "demo.qa_java_csvtoredshift")
			.option("user", "USERNAME HERE")
			.option("password", "PASSWORD HERE").save();
		
		spark.stop();
	}
	
	public static void main(String[] args) {
		new Spark_CsvToRedshift().doReadCsvToRedshift();
	}
}

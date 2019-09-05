/*
Java Spark Sample Source code Accessing Amazon Redshift Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/


package com.java.lineage.redshift;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Spark_RedshiftToParquet {

	public void doReadRedshiftToParquet() {
		SparkSession spark = SparkSession.builder().appName("Java Spark Redshift basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		
		Dataset<Row> jdbcDF_r7 = null;
		jdbcDF_r7 = spark.read().format("jdbc")
				.option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world")
				.option("driver","com.amazon.redshift.jdbc.Driver")
				.option("dbtable", "public.employee_details")
				.option("user", "USERNAME HERE")
				.option("password", "PASSWORD HERE").load();
		
		jdbcDF_r7.write().parquet("s3a://asgcombilling/PySpark/Source/rubiks.parquet");
		//jdbcDF.write().parquet("C:/RedShift/Raghav/LineageTargetFiles/user/root/java/QATarget/QA_JAVA_redshifttoparquetfile.parquet");
		
		spark.stop();
	}
	
	public static void main(String[] args) {
		new Spark_RedshiftToParquet().doReadRedshiftToParquet();
	}
}
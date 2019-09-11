/*
Java Spark Sample Source code Accessing Amazon Redshift Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/



package com.java.lineage.redshift;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Spark_Redshift_Format_Select {

	public void doReadRedShift() {
		SparkSession spark = SparkSession.builder().appName("Java Spark Redshift basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> jdbcDF_r1 = null;
		jdbcDF_r1 = spark.read().format("jdbc")
				.option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world")
				.option("driver","com.amazon.redshift.jdbc.Driver")
				.option("dbtable", "demo.employee")
				.option("user", "USERNAME HERE")
				.option("password", "PASSWORD HERE").load()
				.select("empid", "ename", "address");

		jdbcDF_r1.filter(jdbcDF_r1.col("empid").gt(10)).select("empid", "ename", "address").write()
				.format("jdbc")	
				.option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world")
				.option("driver","com.amazon.redshift.jdbc.Driver")
				.option("dbtable", "demo.qa_javaemployee")
				.option("user", "USERNAME HERE")
				.option("password", "PASSWORD HERE").save();

	}
	
	public static void main(String[] args) {
		new Spark_Redshift_Format_Select().doReadRedShift();
	}
}
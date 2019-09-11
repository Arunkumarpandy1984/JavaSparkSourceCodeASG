/*
Java Spark Sample Source code Accessing Amazon Redshift Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/



package com.java.lineage.redshift;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Spark_Redshift_JDBC {
	
	public void doReadRedshiftJDBC() {
		SparkSession spark = SparkSession.builder().appName("Java Spark Redshift basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		
		Dataset<Row> jdbcDF_r2 = null;
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "USERNAME HERE");
		connectionProperties.put("password", "PASSWORD HERE");
		connectionProperties.put("driver","com.amazon.redshift.jdbc.Driver");
		
		jdbcDF_r2 = spark.read().jdbc("jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world", 
				"demo.employee", connectionProperties);
		
		jdbcDF_r2.write().jdbc("jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world", 
				"demo.writeqa_employee", connectionProperties);
		
		spark.stop();
	}
	
	public static void main(String[] args) {
		new Spark_Redshift_JDBC().doReadRedshiftJDBC();
	}


}
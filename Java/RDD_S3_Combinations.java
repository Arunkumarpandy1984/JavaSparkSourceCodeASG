/*
Java Spark Sample Source code Accessing Amazon S3 Data Source.
	Copyright © 2019 ASG Technologies Group, Inc. All rights reserved.
*/


package com.java.lineage.s3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDD_S3_Combinations {
	
	public void fileToDirectory_txt() {
		SparkConf conf = new SparkConf().setAppName("Java Spark AWS S3 Example");
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		JavaRDD<String> lines = sc.textFile("s3a://asgcoms3bucket/testFiles/JavaSpark/Source/rddSource.txt");
		lines.saveAsTextFile("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/rddTarget_FileToDirectory");
		sc.close();
	}
	
	public void directoryToFile_csv() {
		SparkConf conf = new SparkConf().setAppName("Java Spark AWS S3 Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines1 = sc.textFile("s3a://asgcoms3bucket/testFiles/Java/Destination");
		lines1.saveAsTextFile("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/output_DirectoryToFile.csv");
		sc.close();

	}
	
	public void directoryToDirectory() {
		SparkConf conf = new SparkConf().setAppName("Java Spark AWS S3 Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines2 = sc.textFile("s3a://asgcoms3bucket/testFiles/Java/Destination");
		lines2.saveAsTextFile("s3a://asgcoms3bucket/testFiles/JavaSpark/JavaTarget/output_DirectoryToDirectory");	
		sc.close();

	}
	
	public static void main(String[] args) {
		new RDD_S3_Combinations().fileToDirectory_txt();
		new RDD_S3_Combinations().directoryToFile_csv();
		new RDD_S3_Combinations().directoryToDirectory();
	}

}
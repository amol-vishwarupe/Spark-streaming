package com.spark.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import  com.ipl.postgresql.*;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class filestreaming {

	public static void main(String[] args) throws StreamingQueryException {
		// TODO Auto-generated method stub
		
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		SparkSession spark = SparkSession.builder()
										 .appName("Streamin")
										 .master("local")
										 .getOrCreate();
		
		StructType userSchema  = new StructType().add("name" ,"String").add("score" ,"int");
		
		Dataset<Row>  stockData = spark
								.readStream()
								.option("sep", ",")
								.option("header", true)
								.schema(userSchema)
								.csv("D:\\Code_BigData\\sparkJava\\workspace\\project1\\src\\main\\resources\\source")
								;
		
		
		 Dataset<Row> resultDf = stockData.withColumn("name", stockData.col("name"))
				 						   .withColumn("score", stockData.col("score"));
		 
		 /*
		 
		 StreamingQuery query = resultDf.writeStream()
				 						.format("csv")
				 						.option("checkpointLocation", "D:\\Code_BigData\\sparkJava\\workspace\\project1\\src\\main\\resources\\chekpoint")
				 						.option("path","D:\\Code_BigData\\sparkJava\\workspace\\project1\\src\\main\\resources\\sink")
				 						.start();
		
		
		*/
		 
		 // Writing to  RDBMS (postgreSQL) using JDBC
		 
		 StreamingQuery query = resultDf.writeStream().
				 						outputMode(OutputMode.Append())
				 						.foreach(new ForeachWriter<Row>() {
				 				              @Override
				 				              public boolean open(long partitionId, long version) {
				 				                System.out.println("partitionId:" + partitionId + "version:" + version);
				 				                return true;
				 				              }

				 				              @Override
				 				              public void process(Row value) {
				 				                System.out.println("process:" + value.getString(0));
				 				                System.out.println("process:" + String.valueOf(value.getInt(1)));
				 				                
				 				               InsertIntoPosrgresql a = new InsertIntoPosrgresql();
				 				               String id =  a.insertActor(new Player(value.getString(0) ,value.getInt(1)));
				 				              
				 				              System.out.println("return value :  " +id);
				 				              }

				 				              @Override
				 				              public void close(Throwable errorOrNull) {
				 				                System.out.println("close");
				 				              }
				 				            }).start();
		 
		 query.awaitTermination();
		

	}

}

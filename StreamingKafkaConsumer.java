package com.kafka.streaming;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingKafkaConsumer {

	public static void main(String[] args) throws StreamingQueryException {
		// TODO Auto-generated method stub
		
		Logger logger = Logger(Level.ERROR );

		SparkSession spark = new SparkSession
									.Builder()
									.appName("Kafka consumer")
									.master("local")
									.getOrCreate();
		
		
		Dataset<Row> messageDF = spark.readStream()
									  .format("kafka")
									  .option("kafka.bootstrap.servers" ,"localhost:9092")
									  .option("subscribe", "test")
									  .load()
									  .selectExpr("CAST(value AS STRING)");
		
		Dataset<String> words = messageDF
								.as(Encoders.STRING())
								.flatMap((FlatMapFunction<String , String>) x -> Arrays.asList(x.split(" ")).iterator() , Encoders.STRING());
		
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		
		StreamingQuery query = wordCounts.writeStream()
										 .outputMode(OutputMode.Complete())
										 .format("console")
										 .start();
		
		
		query.awaitTermination();
								
								
		
		
		
	}

	private static Logger Logger(Level error) {
		// TODO Auto-generated method stub
		return null;
	}

}

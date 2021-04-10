package com.RUSpark;

import org.apache.spark.sql.SparkSession;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		SparkSession spark = SparkSession
            .builder()
            .appName("RedditPhotoImpact")
            .getOrCreate();
		
	}

}

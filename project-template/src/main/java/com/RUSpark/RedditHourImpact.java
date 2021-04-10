package com.RUSpark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		JavaRDD<String[]> colVals = lines.map(line -> 
          line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1));
		
		JavaRDD<String[]> hourImpacts = colVals.map((n) -> calcHourImpact(n));

		Integer[][] temp = new Integer[24][2];
		for(int i = 0; i < temp.length; i++) {
		  temp[i][0] = i;
		  temp[i][1] = 0;
		}
		
		JavaPairRDD<Integer, Integer> hourImpactFormat = 
            hourImpacts.mapToPair(s -> new Tuple2<>(Integer.valueOf(s[0]), Integer.valueOf(s[1])));
		
		JavaPairRDD<Integer, Integer> hourSum = hourImpactFormat.reduceByKey((i1, i2) -> i1 + i2);
		hourSum = hourSum.sortByKey();
		
		List<Tuple2<Integer, Integer>> output = hourSum.collect();
		System.out.println("Printing Not Zero: ");
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + " " + tuple._2());
        }
		int lastHour = -1;
		System.out.println("Printing All: ");
        for (Tuple2<?,?> tuple : output) {
          printPrev(lastHour, (Integer) tuple._1());
          System.out.println(tuple._1() + " " + tuple._2());
          lastHour = (Integer) tuple._1();
        }
        
        if(lastHour < 23) {
          while(lastHour != 23) {
            lastHour++;
            System.out.println(lastHour + " 0");
          }
        }
		
	}
	
	private static void printPrev(int lastHour, Integer crntHour) {
	   if(crntHour.intValue() - lastHour <= 1) 
	     return;
	   int temp = lastHour;
	   while(lastHour != crntHour.intValue()) {
	     lastHour++;
	     if(lastHour == crntHour)
	       break;
	     System.out.println(lastHour+" 0");
	   }
	}
	
	   private static String[] calcHourImpact(String[] row) {
	      String[] updatedPostImpact = new String[2];
	      Integer tempTime = Integer.valueOf(row[1]);
	      int hours = new java.util.Date((long)tempTime*1000).getHours();
	      updatedPostImpact[0] = String.valueOf(hours);
	      updatedPostImpact[1] = String.valueOf((Integer.valueOf(row[4]) +
	          Integer.valueOf(row[5]) + Integer.valueOf(row[6])));
	      return updatedPostImpact;
	    }

}

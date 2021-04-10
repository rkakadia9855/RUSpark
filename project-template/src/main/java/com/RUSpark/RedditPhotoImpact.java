package com.RUSpark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
/* any necessary Java packages here */
import scala.Tuple2;

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		SparkSession spark = SparkSession
		      .builder()
		      .appName("RedditPhotoImpact")
		      .getOrCreate();
		
	    ArrayList<String> cols = new ArrayList<String>();
	    cols.add("#image_id");
	    cols.add("unixtime");
	    cols.add("title");
	    cols.add("subreddit");
	    cols.add("number_of_upvotes");
	    cols.add("number_of_downvotes");
	    cols.add("number_of_comments");
	    
	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
	    
	    JavaRDD<String[]> colVals = lines.map(line -> 
	      line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1));
	    
	    List<String[]> temp = colVals.collect();
	    int row = 1;
	    for(int i = 0; i < temp.size(); i++) {
	      if(temp.get(i).length != 7)
	        System.out.println("Invalid number of cols in row "+ row);
	      row++;
	    }

	    JavaRDD<String[]> postImpacts = colVals.map((n) -> calcPostImpact(n));

	    JavaPairRDD<Integer, Integer> postImpactKV = 
	        postImpacts.mapToPair(s -> new Tuple2<>(Integer.valueOf(s[0]), Integer.valueOf(s[1])));
	    
	    List<Tuple2<Integer, Integer>> tempOut = postImpactKV.collect();
	    System.out.println("-------------------------------------");
	    System.out.println("Printing post impact start");
        for (Tuple2<?,?> tuple : tempOut) {
          System.out.println(tuple._1() + " " + tuple._2());
        }
        System.out.println("Printing post impact end");
        System.out.println("-------------------------------------");
        
	    JavaPairRDD<Integer, Integer> postImpact = postImpactKV.reduceByKey((i1, i2) -> i1 + i2);
	    
	    List<Tuple2<Integer, Integer>> output = postImpact.collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
		
	}
	
	private static String[] calcPostImpact(String[] row) {
      String[] updatedPostImpact = new String[2];
      updatedPostImpact[0] = row[0];
      updatedPostImpact[1] = String.valueOf((Integer.valueOf(row[4]) +
          Integer.valueOf(row[5]) + Integer.valueOf(row[6])));
      return updatedPostImpact;
	}

}

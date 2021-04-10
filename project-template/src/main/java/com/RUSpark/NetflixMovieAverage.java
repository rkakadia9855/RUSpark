package com.RUSpark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
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
        
        JavaRDD<String[]> keepTwoCol = colVals.map((n) -> removeCols(n));
        
        JavaPairRDD<Integer, List<Double>> movieRatings = 
            keepTwoCol.mapToPair(s -> new Tuple2<>(Integer.valueOf(s[0]),
                Arrays.asList(Double.valueOf(s[1]), 1.0)));
        
        JavaPairRDD<Integer, List<Double>> sumRatings = movieRatings.reduceByKey((i1, i2) -> 
          Arrays.asList(i1.get(0) + i2.get(0), i1.get(1) + i2.get(1)));
        
        JavaPairRDD<Integer, Double> averageRatings = sumRatings.mapToPair(s -> new 
            Tuple2<>(s._1, s._2.get(0)/s._2.get(1)));

        List<Tuple2<Integer, Double>> finalAverageRatings = averageRatings.collect();

        for (Tuple2<?,?> tuple : finalAverageRatings) {
          System.out.println(tuple._1() + " " + tuple._2());
        }
        
        spark.stop();

	}
	
	private static String[] removeCols(String[] colVals) {
	  String[] dropped = new String[2];
	  dropped[0] = colVals[0];
	  dropped[1] = colVals[2];
	  return dropped;
	}

}

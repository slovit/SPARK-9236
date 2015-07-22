package org.slovit.spark;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple2;

public class SparkTest {
	
	/**
	 * Failing
	 */
	@Test
	public void joinEmptyRDDTest() {
		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
			JavaRDD<String> rdd1 = sparkContext.parallelize(Collections.singletonList("one"));
			JavaRDD<String> rdd2 = sparkContext.emptyRDD();
			
			JavaPairRDD<Integer, String> pair1 = rdd1.mapToPair(t -> new Tuple2<Integer, String>(1, t));
			JavaPairRDD<Integer, Iterable<String>> pair2 = rdd2.groupBy(t -> 2);
			
			Assert.assertFalse(pair1.leftOuterJoin(pair2).collect().isEmpty());
		}
	}
	
	/**
	 * Expected behavior
	 */
	@Test
	public void joinEmptyRDDTest_success() {
		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
			JavaRDD<String> rdd1 = sparkContext.parallelize(Collections.singletonList("one"));
			// The only difference between the tests
			JavaRDD<String> rdd2 = sparkContext.parallelize(Collections.singletonList("two"));
			
			JavaPairRDD<Integer, String> pair1 = rdd1.mapToPair(t -> new Tuple2<Integer, String>(1, t));
			JavaPairRDD<Integer, Iterable<String>> pair2 = rdd2.groupBy(t -> 2);
			
			Assert.assertFalse(pair1.leftOuterJoin(pair2).collect().isEmpty());
		}
	}

}

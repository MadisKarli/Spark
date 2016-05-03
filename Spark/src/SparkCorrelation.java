import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

public class SparkCorrelation {
	public static void main(String[] args) {
		 final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Spark Correlation");
		conf.set("eventLog.enabled", "false");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> data = jsc.textFile(args[0]);
		/*
		 * There are two ways to map the data 
		 * First: map both X and Y separately but Spark does not optimize this enough 
		 * even though they should be  able to work together 
		 * Second: map to one PairRDD and take keys as X, values as Y 
		 */

		JavaPairRDD<Double, Double> XY = data.mapToPair(new PairFunction<String, Double, Double>() {
			private static final long serialVersionUID = -3396355538730137408L;

			@Override
			public Tuple2<Double, Double> call(String row) throws Exception {
				String[] split = row.split("\t");
				double x = Double.parseDouble(split[4]);
				double y = Double.parseDouble(split[5]);
				return new Tuple2<Double, Double>(x, y);
			}
		});
		JavaRDD<Double> X = XY.keys();
		JavaRDD<Double> Y = XY.values();
		//pearson method is default, other is spearman method
		Double correlation = Statistics.corr(X, Y);
		
		
		final long endTime = System.currentTimeMillis();
		List<Double> answer = new ArrayList<Double>();
		answer.add((double) (endTime-startTime));
		answer.add(correlation);
		JavaDoubleRDD out = jsc.parallelizeDoubles(answer);
		out.saveAsTextFile(args[0]+String.valueOf(endTime) +"Spark Correlation Out");
		jsc.close();
		System.out.println("Execution time: " + (endTime - startTime) );
		System.out.println("Correlation is: " + correlation);
	}
}
/*
 * orig
 * 0.9202032394442802
 * Execution time: 6721 using XY
 * 1 mil 
 * Execution time: 15656 using two maps 
 * Execution time: 12164using XY and stuff 
 * 5mil 
 * Execution time: 61453 two maps 
 * Execution time: 46945 XY
 * 10mil
 * Execution time: 157291 XY
 */
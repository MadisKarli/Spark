
//https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/mllib/JavaNaiveBayesExample.java
//https://github.com/apache/spark/commit/2804674a7af8f11eeb1280459bc9145815398eed
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class SparkBayes {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setAppName("Naive Bayes in Spark on " + args[0]);
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//uncomment to turn loggers off
//		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//		loggers.add(LogManager.getRootLogger());
//		for ( Logger logger : loggers ) {
//		    logger.setLevel(Level.ERROR);
//		}
		JavaRDD<String> data = jsc.textFile(args[0],8);
		JavaRDD<LabeledPoint> inputData = data.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = 7004546027616594543L;

			public LabeledPoint call(String line) {
				String[] parts = line.split(",");
				double[] v = new double[2];
				v[0] = Double.parseDouble(parts[1]);
				v[1] = Double.parseDouble(parts[2]);
				return new LabeledPoint(Double.parseDouble(parts[55]), Vectors.dense(v));
				
			}
		});
		inputData.cache();
		JavaRDD<LabeledPoint>[] split = inputData.randomSplit(new double[] { 0.8, 0.2 });
		JavaRDD<LabeledPoint> training = split[0]; // training set
		JavaRDD<LabeledPoint> test = split[1]; // test set change to tmp[1]
		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 0.5);
		JavaPairRDD<Double, Double> predictionAndLabel = test
				.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					private static final long serialVersionUID = 5970068786622801541L;

					@Override
					public Tuple2<Double, Double> call(LabeledPoint p) {
						return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
					}
				});
		final long modelTime = System.currentTimeMillis();
		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			private static final long serialVersionUID = -7042070591367537117L;

			@Override
			public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1().equals(pl._2());
			}
		}).count() / (double) test.count();
		
		
		
		
		System.out.println("training size " + training.count() + " test size " + test.count());
		System.out.println("accuracy " + accuracy);
		final long endTime = System.currentTimeMillis();
		List<Double> answer = new ArrayList<Double>();
		answer.add(accuracy);
		answer.add((double) training.count());
		answer.add((double) test.count());
		answer.add((double) (modelTime - startTime));
		answer.add((double) (endTime - startTime));
		
		JavaDoubleRDD out = jsc.parallelizeDoubles(answer);
		out.saveAsTextFile(args[0] +" "+ String.valueOf(endTime) +" Spark Bayes Out " +String.valueOf(inputData.count()));
		
		System.out.println("Model creation time: " + (modelTime - startTime));
		System.out.println("Total execution time: " + (endTime - startTime));
		jsc.close();
	}
}

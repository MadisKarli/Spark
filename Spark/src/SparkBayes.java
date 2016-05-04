
//https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/mllib/JavaNaiveBayesExample.java
//https://github.com/apache/spark/commit/2804674a7af8f11eeb1280459bc9145815398eed
// $example on$
import java.util.ArrayList;
import java.util.List;

// $example off$
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
		SparkConf sparkConf = new SparkConf().setAppName("JavaNaiveBayesExample");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		// c) If you skip one in between, it should be assigned a default value
		// of zero.
		// In short, +1 1:0.7 2:1 3:1 translates to:
		// Assign to class +1, the point (0.7,1,1).
		JavaRDD<String> data = jsc.textFile(args[0],8);
		JavaRDD<LabeledPoint> inputData = data.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = 7004546027616594543L;

			public LabeledPoint call(String line) {
				String[] parts = line.split(",");
				double[] v = new double[2];
				v[0] = Double.parseDouble(parts[1]);
				v[1] = Double.parseDouble(parts[54]);
				return new LabeledPoint(Double.parseDouble(parts[55]), Vectors.dense(v));
				
			}
		});
		inputData.cache();
		//String path = "data/bayes spark3.txt";
//		JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
		JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[] { 0.8, 0.2 }, 12345);
		JavaRDD<LabeledPoint> training = tmp[0]; // training set
		JavaRDD<LabeledPoint> test = tmp[1]; // test set
		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
		JavaPairRDD<Double, Double> predictionAndLabel = test
				.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					private static final long serialVersionUID = 5970068786622801541L;

					@Override
					public Tuple2<Double, Double> call(LabeledPoint p) {
//						System.out.println(p.features() + " actual " + p.label() + " prediction " + model.predict(p.features()));
						return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
					}
				});
		
		
		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			private static final long serialVersionUID = -7042070591367537117L;

			@Override
			public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1().equals(pl._2());
			}
		}).count() / (double) test.count();
		
		
		
		
		System.out.println("training size " + training.count() + " test size " + test.count());
		System.out.println("accuracy " + accuracy);
		System.out.println();
		List<Double> answer = new ArrayList<Double>();
		for (double el : model.pi()) {
			answer.add(el);
//			System.out.println(el);
		}
		// Save and load model
		// model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
		final long endTime = System.currentTimeMillis();
		JavaDoubleRDD out = jsc.parallelizeDoubles(answer);
		out.saveAsTextFile(args[0] + String.valueOf(endTime) +"Spark Bayes Out " +String.valueOf(inputData.count()));
		System.out.println("Total execution time: " + (endTime - startTime));
		jsc.close();
		//acc on original data - 90/10 - 54%, 80/20 -60%
	}
}

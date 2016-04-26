import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import antlr.collections.impl.Vector;
//https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/ml/JavaLinearRegressionWithElasticNetExample.java
//optimization http://spark.apache.org/docs/latest/mllib-optimization.html

//üks võimalus - http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SparkLinearRegression {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Spark Linear regression");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
			logger.setLevel(Level.ERROR);
		}
		// Load and parse the data
		String path = "input/youtube_videos_small.tsv";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<LabeledPoint> parsedData = data.map(
				new Function<String, LabeledPoint>() {
					public LabeledPoint call(String line) {
						String[] parts = line.split("\t");
						double[] v = new double[1];
						v[0] = Double.parseDouble(parts[4]);
						return new LabeledPoint(Double.parseDouble(parts[5]), Vectors.dense(v));
					}
				}
				);
		parsedData.cache();
		System.out.println(parsedData.count());

		// Building the model
		int numIterations = 11;
		double stepSize = 0.001;
		//http://stackoverflow.com/questions/26259743/spark-mllib-linear-regression-model-intercept-is-always-0-0
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer()
		.setNumIterations(numIterations)
		.setStepSize(stepSize);
		final LinearRegressionModel model = alg.run(JavaRDD.toRDD(parsedData));
		
		System.out.println(model.intercept() + " intercept, weights " + model.weights());
		
		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
				new Function<LabeledPoint, Tuple2<Double, Double>>() {
					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
						System.out.println("prediction: " + prediction + " actual " + point.label() + " features " + point.features() + " point " + point);
						return new Tuple2<>(prediction, point.label());
					}
				}
				);
		double MSE = new JavaDoubleRDD(valuesAndPreds.map(
				new Function<Tuple2<Double, Double>, Object>() {
					public Object call(Tuple2<Double, Double> pair) {
						return Math.pow(pair._1() - pair._2(), 2.0);
					}
				}
				).rdd()).first();
		System.out.println("training Mean Squared Error = " + MSE);

		// Save and load model
//			    model.save(sc.sc(), "target/tmp/javaLinearRegressionWithSGDModel");
//			    LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(),
//			      "target/tmp/javaLinearRegressionWithSGDModel");
		final long endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime) );
		sc.stop();
	}
}
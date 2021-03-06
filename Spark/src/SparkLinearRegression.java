import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

// $example on$
import scala.Tuple2;

public class SparkLinearRegression {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Spark Linear Regression on " + args[0]);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
//		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//		loggers.add(LogManager.getRootLogger());
//		for ( Logger logger : loggers ) {
//		    logger.setLevel(Level.ERROR);
//		}
		
		
		// Load and parse the data
		JavaRDD<String> data = jsc.textFile(args[0],8);
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = 7004546027616594543L;

			public LabeledPoint call(String line) {
				String[] parts = line.split("\t");
				double[] v = new double[1];
				v[0] = Double.parseDouble(parts[4])/500;
				return new LabeledPoint(Double.parseDouble(parts[5])/500, Vectors.dense(v));
			}
		});
		parsedData.cache();
		
		JavaRDD<LabeledPoint>[] split = parsedData.randomSplit(new double[] { 0.8, 0.2 });
		JavaRDD<LabeledPoint> training = split[0]; // training set
		JavaRDD<LabeledPoint> test = split[1]; // test set change to tmp[1]

		// Building the model
		int numIterations = 100;
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer().setNumIterations(numIterations);
		final LinearRegressionModel model = alg.run(JavaRDD.toRDD(training));
		

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = test
				.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
					private static final long serialVersionUID = -1107559998389840064L;

					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
						return new Tuple2<>(prediction, point.label());
					}
				});
		final long modelTime = System.currentTimeMillis();
		
		double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Double, Double>, Object>() {
			private static final long serialVersionUID = -1068039678092009862L;
			public Object call(Tuple2<Double, Double> pair) {
				return Math.pow(pair._1()*500 - pair._2()*500, 2.0);
			}
		}).rdd()).mean();

		
		
		final long endTime = System.currentTimeMillis();
		List<Double> answer = new ArrayList<Double>();
		answer.add(model.intercept());
		answer.add(model.weights().toArray()[0]);
		answer.add(MSE);
		answer.add((double) parsedData.count());
		answer.add((double) (modelTime-startTime));
		answer.add((double) (endTime-startTime));
		JavaDoubleRDD out = jsc.parallelizeDoubles(answer);
		out.saveAsTextFile(args[0]+" "+String.valueOf(endTime) +" Spark linear regression out " + String.valueOf(parsedData.count()));
		System.out.println(model.intercept() + " intercept, weights " + model.weights());
		System.out.println("training Mean Squared Error = " + MSE);
		System.out.println("Model creation time: " + (modelTime - startTime));
		System.out.println("Execution time: " + (endTime - startTime));
		jsc.close();
	}
}
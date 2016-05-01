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

//http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SparkLinearRegression {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Spark Linear regression");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		// Load and parse the data
		JavaRDD<String> data = jsc.textFile(args[0]);
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
		
		// Building the model
		int numIterations = 600;
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer().setNumIterations(numIterations);
		final LinearRegressionModel model = alg.run(JavaRDD.toRDD(parsedData));
		

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData
				.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
					private static final long serialVersionUID = -1107559998389840064L;

					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
//						System.out.println("prediction: " + prediction * 500  + " actual " + point.label() * 500  + " features "
//								+ point.features() + " point " + point);
						return new Tuple2<>(prediction, point.label());
					}
				});
		double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Double, Double>, Object>() {
			private static final long serialVersionUID = -1068039678092009862L;

			public Object call(Tuple2<Double, Double> pair) {
				return Math.pow(pair._1() - pair._2(), 2.0);
			}
		}).rdd()).mean();
		System.out.println(model.intercept() + " intercept, weights " + model.weights());
		System.out.println("training Mean Squared Error = " + MSE);
		System.out.println("count " + parsedData.count());

		final long endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime));
		jsc.stop();
		jsc.close();
		/*
		 * orig	Execution time: 11193
		 * 0.19074499877604628 intercept, weights [0.4925025376328031]
		 */
	}
}
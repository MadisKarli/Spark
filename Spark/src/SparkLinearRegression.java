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

//üks võimalus - http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SparkLinearRegression {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Spark Linear regression");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load and parse the data
		String path = "input/youtube_videos.tsv";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 7004546027616594543L;

			public LabeledPoint call(String line) {
				String[] parts = line.split("\t");
				double[] v = new double[1];
				v[0] = Double.parseDouble(parts[4])/1000;
				return new LabeledPoint(Double.parseDouble(parts[5])/1000, Vectors.dense(v));
			}
		});
		parsedData.cache();
		
		// Building the model
		int numIterations = 11;
		//double stepSize = 0.001;
		// http://stackoverflow.com/questions/26259743/spark-mllib-linear-regression-model-intercept-is-always-0-0
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer().setNumIterations(numIterations);
		final LinearRegressionModel model = alg.run(JavaRDD.toRDD(parsedData));
		//final LinearRegressionModel model2 = LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);
		

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData
				.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -1107559998389840064L;

					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
						System.out.println("prediction: " + prediction * 1000 + " actual " + point.label() * 1000 + " features "
								+ point.features() + " point " + point);
						return new Tuple2<>(prediction, point.label());
					}
				});
		double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Double, Double>, Object>() {
			/**
			 * 
			 */
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
		sc.stop();
		sc.close();
	}
}
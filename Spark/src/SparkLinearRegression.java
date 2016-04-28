import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.mllib.feature.StandardScaler;
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
		List<Logger> loggers = Collections.<Logger> list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for (Logger logger : loggers) {
			logger.setLevel(Level.ERROR);
		}
		// Load and parse the data
		String path = "input/youtube_videos_small.tsv";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String line) {
				String[] parts = line.split("\t");
				double[] v = new double[1];
				v[0] = Integer.parseInt(parts[4]);
				return new LabeledPoint(Integer.parseInt(parts[5]), Vectors.dense(v));
			}
		});
		parsedData.cache();
		System.out.println(parsedData.count());
		// StandardScaler scaler = new StandardScaler(true,
		// true).fit(parsedData.map(x -> x.features()));
		// LinearRegression lr = new
		// LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);
		// ParamMap paramMap = new ParamMap();
		// paramMap.put(lr.maxIter(), 30);
		// paramMap.put(lr.regParam(), 0.3);
		// org.apache.spark.ml.regression.LinearRegressionModel lrModel =
		// lr.fit(data, paramMap);
		// System.out.println("Coefficients: "
		// + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
		// Building the model
		double[] initialW = new double[1];
		initialW[0] = 0.00000000000000000000000001;
		int numIterations = 11;
		double stepSize = 0.001;
		// http://stackoverflow.com/questions/26259743/spark-mllib-linear-regression-model-intercept-is-always-0-0
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer().setNumIterations(numIterations).setStepSize(stepSize);
		final LinearRegressionModel model = alg.run(JavaRDD.toRDD(parsedData), Vectors.dense(initialW));

		JavaRDD<Tuple2<Double, Double>> valuesAndPreds1 = parsedData
				.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
						System.out.println("pede" + prediction);
						return new Tuple2<>(prediction, point.label());
					}
				});

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData
				.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = model.predict(point.features());
						System.out.println("prediction: " + prediction + " actual " + point.label() + " features "
								+ point.features() + " point " + point);
						return new Tuple2<>(prediction, point.label());
					}
				});
		double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Double, Double>, Object>() {
			public Object call(Tuple2<Double, Double> pair) {
				System.out.println("pairs " + pair._1() + " " + pair._2());
				return Math.pow(pair._1() - pair._2(), 2.0);
			}
		}).rdd()).first();
		System.out.println(model.intercept() + " intercept, weights " + model.weights());
		System.out.println("training Mean Squared Error = " + MSE);

		// Save and load model
		// model.save(sc.sc(), "target/tmp/javaLinearRegressionWithSGDModel");
		// LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(),
		// "target/tmp/javaLinearRegressionWithSGDModel");
		final long endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime));
		sc.stop();
	}
}
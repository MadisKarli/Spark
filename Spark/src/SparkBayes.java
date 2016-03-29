
//https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/mllib/JavaNaiveBayesExample.java
//https://github.com/apache/spark/commit/2804674a7af8f11eeb1280459bc9145815398eed
// $example on$
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkBayes {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		System.out.println("Bayessitt");
		SparkConf sparkConf = new SparkConf().setAppName("JavaNaiveBayesExample");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SparkContext sc = jsc.sc();
		//c) If you skip one in between, it should be assigned a default value of zero.
		//In short, +1 1:0.7 2:1 3:1 translates to:
		//Assign to class +1, the point (0.7,1,1).
		String path = "data/bayes spark2.txt";
		JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
		JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4}, 12345);
		JavaRDD<LabeledPoint> training = tmp[0]; // training set
		JavaRDD<LabeledPoint> test = tmp[1]; // test set
		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
		JavaPairRDD<Double, Double> predictionAndLabel =
		  test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		    @Override
		    public Tuple2<Double, Double> call(LabeledPoint p) {
		      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
		    }
		  });
		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		  @Override
		  public Boolean call(Tuple2<Double, Double> pl) {
		    return pl._1().equals(pl._2());
		  }
		}).count() / (double) test.count();
		System.out.println(accuracy);
		
		// Save and load model
//		model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
		final long endTime = System.currentTimeMillis();
		System.out.println("Total execution time: " + (endTime - startTime) );
	}
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.mllib.stat.Statistics;



public class SparkCorrelation {
  public static void main(String[] args) {
	  final long startTime = System.currentTimeMillis();
    SparkConf conf = new SparkConf().setAppName("Spark Correlation");
    conf.set("spark.driver.memory", "14g");
    conf.set("eventLog.enabled", "false");
    conf.set("spark.master", "local");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    
    JavaRDD<String> data = jsc.textFile("python/generated YT 1 million.tsv");

    //IS THERE ANY WAY TO DO THIS IN ONE PASS?
    JavaDoubleRDD X1 = data.mapToDouble(new DoubleFunction<String>(){
		@Override
		public double call(String row) throws Exception {
			return Double.parseDouble(row.split("\t")[4]);
		}

    });
    JavaDoubleRDD Y1 = data.mapToDouble(new DoubleFunction<String>(){
		@Override
		public double call(String row) throws Exception {
			return Double.parseDouble(row.split("\t")[5]);
		}
		
    });
//  JavaDoubleRDD Xs = data.mapToDouble(line -> Double.parseDouble(line.split(",")[0]));
//  JavaDoubleRDD Ys = data.mapToDouble(line -> Double.parseDouble(line.split(",")[1]));
    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default.
    Double correlation = Statistics.corr(X1.srdd(), Y1.srdd(), "pearson");
    
    System.out.println("Correlation is: " + correlation);

    jsc.close();
    final long endTime = System.currentTimeMillis();
	System.out.println("Execution time: " + (endTime - startTime) );
  }
}
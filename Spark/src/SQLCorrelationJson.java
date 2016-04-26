import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SQLCorrelationJson {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Linear Regression in Spark SQL");
			JavaSparkContext jsc = new JavaSparkContext(conf);
			SQLContext sc = new SQLContext(jsc);
			DataFrame a = sc.read().json("data/correlationjson.json");
			a.registerTempTable("Data");
			a = sc.sql("select corr(x,y) Correlation from Data");
			a.show();
			final long endTime = System.currentTimeMillis();
			System.out.println("Execution time: " + (endTime - startTime) );
			jsc.close();
	}
}


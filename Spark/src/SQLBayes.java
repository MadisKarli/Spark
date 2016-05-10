import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SQLBayes {
	//http://nuncupatively.blogspot.com.ee/2011/07/naive-bayes-in-sql.html
	public static void main(String[] args){
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Naive Bayes in Spark SQL on " + args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hc = new HiveContext(sc.sc());
		hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
		
//		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//		loggers.add(LogManager.getRootLogger());
//		for ( Logger logger : loggers ) {
//		    logger.setLevel(Level.ERROR);
//		}
		
		JavaRDD<String> points = sc.textFile(args[0],8); 
		String schemaString = "uid feature1 feature2 class"; //change here
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					private static final long serialVersionUID = 3282431521901483532L;

					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[2], fields[3], fields[55]); //change here
					}
				});
		JavaRDD<Row>[] split = rowRDD.randomSplit(new double[] { 0.8, 0.2 });
		DataFrame data = hc.createDataFrame(split[0], schema);//change to split[0]
		data.registerTempTable("data");
		DataFrame test = hc.createDataFrame(split[1], schema);//change to split[1]
		test.registerTempTable("testdata");
		

		DataFrame a;
		a = hc.sql("drop table agg");
		a = hc.sql("drop table testscores");
		a = hc.sql("drop table predictions");
		
		a = hc.sql("create table agg as "
				+ " select *, class_sum/sum(class_sum) over () as class_prob from "
				+ "("
				+ "select sum(feature1)/count(feature1) as mean1,"
				+ " sum(feature2)/count(feature2) as mean2, "
				+ " variance(feature1) as var1,"
				+ " variance(feature2) as var2, "
				+ " class, count(*) as class_sum "
				+ " from data group by class"
				+ ") s");
		

		a = hc.sql("create table testscores as "
				+ "select uid, t.class tc, a.class ac , "
				+ "(log(class_prob) +"
				+ "log(1/(sqrt(2*3.145*pow(var1,2)))*exp(-pow(feature1-mean1,2)/(2*pow(var1,2)))) + "
				+ "log(1/(sqrt(2*3.145*pow(var2,2)))*exp(-pow(feature2-mean2,2)/(2*pow(var2,2))))"
				+ ") as coef"
				+ " from testdata t "
				+ "inner join agg a on 1 = 1 ");

		final long modelTime = System.currentTimeMillis();
			
		a = hc.sql("create table predictions as "
				+ " Select uid, tc, ac , coef, max(coef) over (partition by uid) as best_coef from testscores");
		

		a = hc.sql("select  sum(if(tc = ac and best_coef = coef, 1, 0))/count(distinct uid) accuracy from predictions");
		a.show();
		//select sum(if(tc = ac and best_coef = coef, 1, 0))/count(distinct uid) acc from predictions
		//https://books.google.ee/books?id=yqhPCwAAQBAJ&pg=PA165&lpg=PA165&dq=spark+predicted+posterior+class+probabilities+from+the+trained+model,+in+the+same+order+as+class+labels&source=bl&ots=iMTMROwPFY&sig=cUI_4H2Br-sLElhbYpvkzd99zTY&hl=et&sa=X&ved=0ahUKEwi_yor2w8PMAhVHWCwKHTlzBRAQ6AEISTAG#v=onepage&q=spark%20predicted%20posterior%20class%20probabilities%20from%20the%20trained%20model%2C%20in%20the%20same%20order%20as%20class%20labels&f=false
		//show correct predictions - for this divide #of correct predictions with # of testdata
		//
		//Model creation time time: 29583
		//Execution time: 47589
		//acc on original data - 306059 / 581012 - 52%
		final long endTime = System.currentTimeMillis();
		System.out.println("Model creation time time: " + (modelTime - startTime) );
		System.out.println("Execution time: " + (endTime - startTime) );
		a.rdd().saveAsTextFile(args[0]+" "+String.valueOf(endTime) +" SQL Bayes out " + String.valueOf(rowRDD.count()));
		sc.close();
		/*after
		 *+------------------+
		 *|          accuracy|
		 *+------------------+
		 *|0.4868104278442702|
		 *+------------------+
		 *before
		 *+------------------+
		 *|          accuracy|
		 *+------------------+
		 *|0.4662694318338148|
		 *+------------------+
		 */
	}
}

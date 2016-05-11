
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
		
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		
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
						return RowFactory.create(fields[0], fields[1], fields[2], fields[55]); //change here
					}
				});
		JavaRDD<Row>[] split = rowRDD.randomSplit(new double[] { 0.8, 0.2 });
		DataFrame data = hc.createDataFrame(split[0], schema);//change to split[0]
		data.registerTempTable("data");
		DataFrame test = hc.createDataFrame(split[1], schema);//change to split[1]
		test.registerTempTable("testdata");
		

		DataFrame a;
//		a = hc.sql("drop table agg");
//		a = hc.sql("drop table coefficients");
//		a = hc.sql("drop table coefficients2");
		a = hc.sql("drop table testscores");
//		a = hc.sql("drop table predictions");
		a = hc.sql("drop table fcoefs");
		a = hc.sql("drop table f2coefs");

		
		//creates table for 
		a = hc.sql("create table fcoefs as select feature1, class, featurecount/classcount as f1coef, classcount/total as ccoef, featurecount, classcount, total from "
				+ "(select feature1, class, sum(value) over (partition by feature1, class) featurecount,sum(value) over (partition by class) classcount, sum(value) over () total from "
				+ "(select feature1, sum(1) value, class from data group by feature1, class)a)b");
//		a = hc.sql("select * from fcoefs");
//		a.show();
		a = hc.sql("create table f2coefs as select feature2, class, featurecount/classcount as f2coef, classcount/total as ccoef, featurecount, classcount, total from "
				+ "(select feature2, class, sum(value) over (partition by feature2, class) featurecount,sum(value) over (partition by class) classcount, sum(value) over () total from "
				+ "(select feature2, sum(1) value, class from data group by feature2, class)a)b");
//		a = hc.sql("select * from f2coefs");
//		a.show();
		final long modelTime = System.currentTimeMillis();
		a = hc.sql("create table testscores as select uid, t.feature1, t.feature2, t.class as actual, a.class as prediction, f1coef, ccoef, f2coef, f1coef*ccoef*f2coef score from testdata t "
				+ "inner join "
				+ "(select feature1, class, f1coef, ccoef from fcoefs)a on t.feature1 = a.feature1 "
				+ "inner join "
				+ "(select feature2, class, f2coef from f2coefs)b on t.feature2 = b.feature2 and a.class = b.class");
//		a = hc.sql("select * from testscores");
//		a.show();
		
		a = hc.sql("select sum(if(actual = prediction, 1, 0))/count(*) accuracy from (select actual, prediction from testscores a "
				+ " inner join (select c.uid, max(score) maxscore from testscores c group by c.uid) b on a.uid = b.uid and a.score = maxscore)a");
		a.show();
				//acc on original data - 67%
		final long endTime = System.currentTimeMillis();
		System.out.println("Model creation time time: " + (modelTime - startTime) );
		System.out.println("Execution time: " + (endTime - startTime) );
		a.rdd().saveAsTextFile(args[0]+" "+String.valueOf(endTime) +" SQL Bayes out " + String.valueOf(rowRDD.count()));
		sc.close();
		/*
		 *+------------------+
		 *|          accuracy|
		 *+------------------+
		 *|0.4662694318338148|
		 *+------------------+
		 */
	}
}
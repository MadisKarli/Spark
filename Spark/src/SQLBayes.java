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
import org.apache.spark.mllib.regression.LabeledPoint;
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
		SparkConf conf = new SparkConf().setAppName("Naive Bayes in SparkSQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hc = new HiveContext(sc.sc());
		hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
		
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		
		JavaRDD<String> points = sc.textFile(args[0],8); 
		//JavaRDD<String> points = sc.textFile("data/naiveBayesData.txt"); 
		String schemaString = "uid feature feature2 class"; //change here
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
		DataFrame data = hc.createDataFrame(split[0], schema);
		data.registerTempTable("data");
		DataFrame test = hc.createDataFrame(split[1], schema);
		test.registerTempTable("testdata");
		
		/*
		 * adding a new feature:
		 * add feature_n to agg
		 * add feature_n to coefficients2
		 * add feature_n to log(value/(feature_count+feature2_count-value))
		 * add feature_n to left join coefficients b on a.feature = b.feature and a.feature2 = b.feature2"
		 */
		DataFrame a;
		a = hc.sql("drop table agg");
		a = hc.sql("drop table coefficients");
		a = hc.sql("drop table coefficients2");
		a = hc.sql("drop table scores");
		a = hc.sql("drop table output");
		a = hc.sql("drop table test");
		a = hc.sql("drop table testscores");
		a = hc.sql("drop table predictions");
		
		a = hc.sql("create table agg as "
				+ "select feature,feature2, sum(1) value, class from data group by feature,feature2, class");
		a = hc.sql("create table test as select feature, feature2, sum(1) value, class from testdata group by feature, feature2, class");
//		a = hc.sql("select * from agg");
//		a.show();
		a = hc.sql("create table coefficients2 as "
				+ "select class, feature, feature2, value, "
				+ "sum (value) over (partition by feature) as feature_count, "
				+ "sum (value) over (partition by feature2) as feature2_count, "
				+ "sum (value) over (partition by class) as class_count, "
				+ "sum (value) over (order by value) as tot "
				+ "from agg");
//		a = hc.sql("select * from coefficients2");
//		a.show(20);

		
		a = hc.sql("create table coefficients  as "
				+ "select *, log(value/(feature_count+feature2_count-value))-log(class_count/(tot-class_count)) coefficient from coefficients2");
//		a = hc.sql("select * from coefficients");
//		a.show();
		
		a = hc.sql("create table testscores as "
				+ "select a.uid, a.class as actual, b.class as prediction, sum(coefficient) score, a.feature,a.feature2 from testdata a "
				+ "left join coefficients b on a.feature = b.feature and a.feature2 = b.feature2 "
				+ "group by a.uid, a.class, b.class, a.feature, a.feature2");
		
//		DataFrame c = hc.sql("create table predictions as "
//				+ "select a.uid, actual, prediction, score from testscores a "
//				+ "inner join (select c.uid, max(score) maxscore from testscores c group by c.uid) b on a.uid = b.uid and a.score = maxscore");
//		
//		c = hc.sql("select b.correct/count(*) accuracy from test left join (select count(*) correct from predictions where actual = prediction) b on 1 = 1 group by b.correct");
//		c.show();
		
		//57902
		//acc on original data - 306059 / 581012 - 52%
		final long endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime) );
		a.rdd().saveAsTextFile(args[0]+String.valueOf(endTime) +" SQL bayes out " + String.valueOf(rowRDD.count()));
		sc.close();
	}
}

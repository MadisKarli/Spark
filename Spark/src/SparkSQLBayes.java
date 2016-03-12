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

public class SparkSQLBayes {
	//http://nuncupatively.blogspot.com.ee/2011/07/naive-bayes-in-sql.html
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("SparkNaive Bayes in SQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		HiveContext hc = new HiveContext(sc.sc());
		
		
		
		JavaRDD<String> points = sc.textFile("data/naiveBayesData.txt"); //http://sqldatamine.blogspot.com.ee/2013/07/classification-using-naive-bayes.html
		String schemaString = "uid feature value"; //change here
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim(), fields[2]); //change here
					}
				});
		
		DataFrame pdf = hc.createDataFrame(rowRDD, schema);
		pdf.registerTempTable("data");
		

		JavaRDD<String> test = sc.textFile("data/naiveBayesClasses.txt");
		String testschemaString = "uid class";
		List<StructField> testfields = new ArrayList<StructField>();
		for(String fieldName : testschemaString.split(" ")){
			testfields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType testschema = DataTypes.createStructType(testfields);
		JavaRDD<Row> testrowRDD = test.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim()); //change here
					}
				});
		
		DataFrame testpdf = hc.createDataFrame(testrowRDD, testschema);
		testpdf.registerTempTable("classes");
		
		
		DataFrame a,b,c,d,e;
//		a = hc.sql("drop table agg");
//		a = hc.sql("drop table agg2");
//		a = hc.sql("drop table d_classes");
//		a = hc.sql("drop table d_features");
//		a = hc.sql("drop table dense_agg");
//		a = hc.sql("drop table class_sizes");
//		a = hc.sql("drop table coefficients");
//		a = hc.sql("drop table scores");
		
		a = hc.sql("create table agg stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/agg' as "
				+ "select a.uid, a.feature, a.value, b.class from data a inner join classes b on a.uid = b.uid");
		a = hc.sql("select * from agg");
		a.show();
		
		//class is not null does not work for some reason, it was working but now it is not :( current workaround is to use no null classes
		a = hc.sql("create table agg2 stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/agg2' as "
				+ "select feature, sum(1) as value, class from agg where class IS NOT null group by feature, class");
		a = hc.sql("select * from agg2");
		a.show();
		
		a = hc.sql("create table d_classes stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/d_classes' as "
				+ " select class from agg2 group by class");
		a = hc.sql("select * from d_classes");
		a.show();
//		
		a = hc.sql("create table d_features stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/d_features' as select feature from agg2 group by feature");
		a = hc.sql("select * from d_features");
		a.show();
		
		
		a = hc.sql("create table dense_agg  stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/dense_agg' as "
				+ "select a.feature, b.class, 0.5 + c.value value from d_features a left join d_classes b on 1=1 join agg2 c on a.feature = c.feature and b.class = c.class");
		a = hc.sql("select * from dense_agg");
		a.show();
		
		
		a = hc.sql("create table class_sizes stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/class_sizes' as "
				+ "select class, sum(value) classtot, b.tot from dense_agg a inner join (select sum(value) tot from dense_agg) b on 1 = 1 group by class, b.tot");
		a = hc.sql("select * from class_sizes");
		a.show();
		
		
		a = hc.sql("create table coefficients stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/coefficients' as "
				+ "select a.*, log(a.value/(b.value-a.value))-log(classtot/(tot-classtot)) coefficient from dense_agg a "
				+ "inner join (select feature, sum(value) as value from dense_agg c group by feature) b on a.feature = b.feature "
				+ "inner join class_sizes d on a.class=d.class");
		a = hc.sql("select * from coefficients");
		a.show();
//		here the a.value and b.value are often the same the same so log does not return the correct value
		
		a = hc.sql("create table scores stored as textfile location '/home/madis/workspace/Spark/tables/naivebayes/scores' as "
				+ "select a.uid, a.class as actual, b.class as prediction, sum(coefficient) score from agg a "
				+ "inner join coefficients b on a.feature = b.feature "
				+ "group by a.uid, a.class, b.class");
		a = hc.sql("select * from scores");
		a.show();
		
		a = hc.sql("select a.uid, actual, prediction, score from scores a inner join (select c.uid, max(score) maxscore from scores c group by c.uid) b on a.uid = b.uid and a.score = maxscore");
		a.show();
		//log(a.value/(b.value-a.value))-log(classtot/(tot-classtot))
		sc.close();
	}
}

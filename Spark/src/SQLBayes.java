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
		SparkConf conf = new SparkConf().setAppName("Naive Bayes in SparkSQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hc = new HiveContext(sc.sc());
		hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
		
		JavaRDD<String> points = sc.textFile(args[0],8); 
		//JavaRDD<String> points = sc.textFile("data/naiveBayesData.txt"); 
		String schemaString = "uid feature class"; //change here
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
						return RowFactory.create(fields[0], fields[1],fields[55]); //change here
					}
				});
		
		DataFrame pdf = hc.createDataFrame(rowRDD, schema);
		pdf.registerTempTable("data");
		

		DataFrame a, b;
		a = hc.sql("drop table agg");
		a = hc.sql("drop table coefficients");
		a = hc.sql("drop table scores");
		
		//class is not null does not work for some reason, it was working but now it is not :( current workaround is to use A <> B
		a = hc.sql("create table agg as "
				+ "select feature, sum(1)+0.5 value, class from data group by feature, class");
//		a = hc.sql("select * from agg");
//		a.show();
		
		a = hc.sql("create table coefficients  as "
				+ "select a.*, log(a.value/(b.value-a.value))-log(d.value/(t.tot-d.value)) coefficient from agg a "
				+ "inner join (select feature, sum(value) as value from agg group by feature) b on a.feature = b.feature "
				+ "inner join (select class, sum(value) as value from agg group by class) d on a.class=d.class "
				+ "inner join (select sum(value) tot from agg) t on 1 = 1");
//		a = hc.sql("select * from coefficients");
//		a.show();
		
		a = hc.sql("create table scores  as "
				+ "select a.uid, a.class as actual, b.class as prediction, sum(coefficient) score from data a "
				+ "inner join coefficients b on a.feature = b.feature "
				+ "group by a.uid, a.class, b.class");
//		a = hc.sql("select * from scores");
//		a.show();
		
		b = hc.sql("select a.uid, actual, prediction, score from scores a "
				+ "inner join (select c.uid, max(score) maxscore from scores c group by c.uid) b on a.uid = b.uid and a.score = maxscore");
//		b.show(100);
		
		final long endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime) );
		b.rdd().saveAsTextFile(args[0]+String.valueOf(endTime) +"SQL bayes out" + String.valueOf(rowRDD.count()));
		sc.close();
	}
}

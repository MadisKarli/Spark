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

public class SQLAsi2 {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("SparkNaive Bayes in SQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		HiveContext hc = new HiveContext(sc.sc());
		
		
		
		JavaRDD<String> points = sc.textFile("data/k-means2.txt"); //http://sqldatamine.blogspot.com.ee/2013/07/classification-using-naive-bayes.html
		String schemaString = "id area rooms price"; //change here
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1], fields[2], fields[3]); //change here
					}
				});
		
		DataFrame pdf = hc.createDataFrame(rowRDD, schema);
		pdf.registerTempTable("raw");
		
		
		DataFrame a;
		//a = hc.sql("drop table data");
		a = hc.sql("drop table class");
//		a = hc.sql("create table data stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/data' as "
//				+ "select id, 'area' attribute, (area-avgarea) / stdevarea value from raw "
//				+ "join (select AVG(area) avgarea from raw) V on (1=1) "
//				+ "join (select STDDEV_SAMP(area) stdevarea from raw) V on (1=1) "
//				+ "group by id, area, avgarea, stdevarea "
//				+ "union all "
//				+ "select id, 'rooms', (rooms - avgrooms) / stdevrooms from raw "
//				+ "join (select AVG(rooms) avgrooms from raw) V on (1=1) "
//				+ "join (select STDDEV_SAMP(area) stdevrooms from raw) V on (1=1) "
//				+ "group by id, rooms, avgrooms, stdevrooms "
//				+ "union all "
//				+ "select id, 'price', (price - avgprice) / stdevprice from raw "
//				+ "join (select AVG(price) avgprice from raw) V on (1=1) "
//				+ "join (select STDDEV_SAMP(price) stdevprice from raw) V on (1=1)"
//				+ "group by id, price, avgprice, stdevprice"); //(area-(AVG(area))) / stddev_samp(area)
//		System.out.println("data");
		a = hc.sql("select * from data");
		a.show();
		
		a = hc.sql("create table class stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/class' as "
				+ "select id, cast(2*rand() as int) from data"); //number of clusters
		System.out.println("class");
		a = hc.sql("select * from class");
		a.show();

		sc.close();
	}
}

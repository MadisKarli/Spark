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
//http://jonisalonen.com/2012/k-means-clustering-in-mysql/
public class SQLAsi {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("SparkNaive Bayes in SQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		HiveContext hc = new HiveContext(sc.sc());
		
		
		
		JavaRDD<String> points = sc.textFile("data/k-means.txt"); 
		String schemaString = "id cluster_id lat lng"; //change here
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
		
		
		DataFrame a,b,c,d,e;
		a = hc.sql("drop table km_clusters");
		a = hc.sql("drop table km_data");
		a = hc.sql("drop table km_data2");
		a = hc.sql("drop table km_clusters2");
		a = hc.sql("create table km_data (id int, cluster_id int, lat double, lng double) stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/km_data'");
		a = hc.sql("insert into km_data select * from raw");
		a = hc.sql("create table km_clusters (id int, lat double, lng double) stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/km_clusters'");
		a = hc.sql("insert into km_clusters select id, lat, lng from km_data LIMIT 2");
		
		System.out.println("kmdata");
		a = hc.sql("select * from km_data");
		a.show();
		System.out.println("kmclusters");
		a = hc.sql("select * from km_clusters");
		a.show();

		
		
		a = hc.sql("create table km_data2 stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/km_data2' as "
				+ "select a.id, b.id cluster_id, a.lat, a.lng from km_data a join km_clusters b on a.lat = b.lat and a.lng = b.lng order by POW(a.lat-b.lat,2) + POW(b.lng-a.lng,2)");
		System.out.println("kmdata2");
		a = hc.sql("select * from km_data2");
		a.show();
//		a = hc.sql("insert overwrite table km_data select a.id, b.id cluster_id, a.lat, a.lng from km_data a join km_clusters b order by POW(a.lat-b.lat,2) + POW(b.lng-a.lng,2) ASC LIMIT 3");
//		System.out.println("kmdata");
//		a = hc.sql("select * from km_data");
//		a.show();
		
		a = hc.sql("create table km_clusters2 stored as textfile location '/home/madis/workspace/Spark/tables/kmeans/km_clusters2' as "
				+ "select c.id, d.lat, d.lng from km_clusters c join (select cluster_id, AVG(lat) lat, AVG(lng) lng from km_data group by cluster_id) d on c.id = d.cluster_id");
		System.out.println("kmclusters2");
		a = hc.sql("select * from km_clusters2");
		a.show();

		sc.close();
	}
}

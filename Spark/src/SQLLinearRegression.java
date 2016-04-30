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

//http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SQLLinearRegression {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Linear Regression in Spark SQL");
			JavaSparkContext sc = new JavaSparkContext(conf);
			HiveContext hc = new HiveContext(sc.sc());
			hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
			
			JavaRDD<String> points = sc.textFile("input/youtube_videos.tsv"); 
			String schemaString = "x y"; //change here
			List<StructField> fields = new ArrayList<StructField>();
			for(String fieldName : schemaString.split(" ")){
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}
			StructType schema = DataTypes.createStructType(fields);
			JavaRDD<Row> rowRDD = points.map(
					new Function<String, Row>(){
						/**
						 * 
						 */
						private static final long serialVersionUID = -5410204604525205517L;

						public Row call(String record) throws Exception {
							String[] fields = record.split("\t");
							return RowFactory.create(fields[4], fields[5].trim()); //change here
						}
					});
			DataFrame data = hc.createDataFrame(rowRDD, schema);
			data.registerTempTable("data");
			
			
			//https://ayadshammout.com/2013/11/30/t-sql-linear-regression-function/
			//b1 is intercept
			DataFrame a;
			a = hc.sql("drop table linearvalues");
			a = hc.sql("drop table output");
//			a = hc.sql("select * from data");
//			a.show();
			a = hc.sql("create table linearvalues as "
					+ "select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) b1, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) b2 from data");
			a = hc.sql("select * from linearvalues");
			a.show();
			
			a = hc.sql("select (b2 * sum(y) + b1 * sum(x*y) - sum(y) * sum(y)/count(*))/(sum(y*y)-sum(y) * sum(y) / count(*)) R2 from data, linearvalues group by b1,b2");
			a.show();
			
			
			a = hc.sql("create table output as "
					+ "select d.x, l.b2+d.x*l.b1 prediction, d.y actual from data d join linearvalues l");
			a = hc.sql("select * from output");
			a.show();
			
			final long endTime = System.currentTimeMillis();
			System.out.println("Execution time: " + (endTime - startTime) );
			sc.close();
	}
}


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//-Dspark.master=local
public class SparkSQL {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("SparkSQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext hc1 = new SQLContext(sc);
		HiveContext hc = new HiveContext(sc.sc());
		
		JavaRDD<String> points = sc.textFile("points.txt");
		String schemaString = "x y z";
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim(), fields[2]);
					}
				});
		
		DataFrame pdf = hc.createDataFrame(rowRDD, schema);
		pdf.registerTempTable("points");
		
		DataFrame a = hc.sql("select * from loc");
		a.show();
//		a = hc.sql("drop table loc");
//		
//		DataFrame b = hc.sql("CREATE TABLE IF NOT EXISTS loc (y string) STORED AS TEXTFILE LOCATION '/home/madis/workspace/Spark/tables'");
//		b.show();
//		b = hc.sql("insert into loc select x+y from points");
//		b = hc.sql("select * from loc");
//		b.show();
//		
	}
}

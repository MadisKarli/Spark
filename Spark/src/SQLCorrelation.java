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

public class SQLCorrelation {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Correlation in Spark SQL");
			JavaSparkContext sc = new JavaSparkContext(conf);
			//HiveContext hc = new HiveContext(sc.sc());
			SQLContext hc = new SQLContext(sc);
			
			JavaRDD<String> points = sc.textFile("python/generated YT 1 million.tsv"); 
			String schemaString = "x y"; //change here
			List<StructField> fields = new ArrayList<StructField>();
			for(String fieldName : schemaString.split(" ")){
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}
			StructType schema = DataTypes.createStructType(fields);
			JavaRDD<Row> rowRDD = points.map(
					new Function<String, Row>(){
						public Row call(String record) throws Exception {
							String[] fields = record.split("\t");
							return RowFactory.create(fields[4], fields[5]); //change here
						}
					});
			DataFrame data = hc.createDataFrame(rowRDD, schema);
			data.registerTempTable("Data");
			//ra
			DataFrame a = hc.sql("select corr(x,y) Correlation from Data");
			a.show();
			
			final long endTime = System.currentTimeMillis();
			System.out.println("Execution time: " + (endTime - startTime) );
			sc.close();
	}
}


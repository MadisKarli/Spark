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

public class SQLCorrelationHiveContext {
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Correlation in SparkaSQL using HiveContext");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		HiveContext hc = new HiveContext(jsc.sc());

		JavaRDD<String> points = jsc.textFile(args[0],8);
		String schemaString = "x y";
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(new Function<String, Row>() {
			private static final long serialVersionUID = 6843419572422549318L;

			public Row call(String record) throws Exception {
				String[] fields = record.split("\t");
				return RowFactory.create(fields[4], fields[5]);
			}
		});
		DataFrame data = hc.createDataFrame(rowRDD, schema);
		data.registerTempTable("Data");
		DataFrame a = hc.sql("select corr(x,y) Correlation from Data");
		a.show();

		final long endTime = System.currentTimeMillis();
		a.rdd().saveAsTextFile((args[0]+String.valueOf(endTime) +"SQL correlation hivecontext out ")+String.valueOf(rowRDD.count()));
		System.out.println("Execution time: " + (endTime - startTime));
		jsc.close();

	}
}

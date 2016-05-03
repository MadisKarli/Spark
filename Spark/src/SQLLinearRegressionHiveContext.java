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
public class SQLLinearRegressionHiveContext {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Linear Regression in SparkSQL using HiveContext");
			conf.set("eventLog.enabled", "false");
			JavaSparkContext jsc = new JavaSparkContext(conf);
			HiveContext hc = new HiveContext(jsc.sc());
			hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
			
			JavaRDD<String> points = jsc.textFile(args[0],8); 
			String schemaString = "x y";
			List<StructField> fields = new ArrayList<StructField>();
			for(String fieldName : schemaString.split(" ")){
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}
			StructType schema = DataTypes.createStructType(fields);
			JavaRDD<Row> rowRDD = points.map(
					new Function<String, Row>(){
						private static final long serialVersionUID = -5410204604525205517L;

						public Row call(String record) throws Exception {
							String[] fields = record.split("\t");
							return RowFactory.create(fields[4], fields[5]); 
						}
					});
			DataFrame data = hc.createDataFrame(rowRDD, schema);
			data.registerTempTable("data");
			
			
			//https://ayadshammout.com/2013/11/30/t-sql-linear-regression-function/
			/*
			 * SparkSQL (as of 1.6) does not support storing values in variables. This can be overcome by storing values in minitables, storing variables is also possible when using shell.
			 * Following uses 4*count(*), 2*sum(x) and sum(y). Let's hope Optimization gods are smart enough to see this.
			 */
			/*
			 * 160 000 - 
			 * hc Execution time: 16707
			 * sql Execution time: 7003
			 * i - 0.5158112426930667, s - 79.01994446493904
			 * 1mil Execution time: 22186
			 * 0.5294229840839438|90.2586273422204
			 * 3mil Execution time: 33415
			 * |0.529181703766521|90.39266677511154|
			 * 5mil Execution time: 42135
			 * |0.5291844146668602|90.33024924257904|
			 * 10mil Execution time: 65569
			 * |0.5292429389608567|90.29940083047|
			 */
			DataFrame a;
//			a = hc.sql("drop table linearvalues");
//			a = hc.sql("drop table output");
			a = hc.sql("select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) intercept, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) slope from data");
			a.show();
			
			//test predictions
//			a = hc.sql("create table output as "
//					+ "select d.x, l.slope+d.x*l.intercept prediction, d.y actual from data d join linearvalues l");
//			a = hc.sql("select * from output");
//			a.show();
			
			//this calculates R2
//			a = hc.sql("select (slope * sum(y) + intercept * sum(x*y) - sum(y) * sum(y)/count(*))/(sum(y*y)-sum(y) * sum(y) / count(*)) R2 from data, linearvalues group by intercept,slope");

			
			final long endTime = System.currentTimeMillis();
			a.rdd().saveAsTextFile((args[0]+String.valueOf(endTime) +"SQL linearregression hivecontext out"));
			System.out.println("Execution time: " + (endTime - startTime) );
			jsc.close();
	}
}

